package olcrtc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
	"github.com/xtaci/smux"
)

var ErrManagerClosed = errors.New("olcrtc: manager closed")
var ErrManagerStreamLimit = errors.New("olcrtc: manager stream limit reached")

const (
	defaultMaxConcurrentStreams = 128
)

// ManagerState describes the lifecycle stage of a persistent olcRTC carrier.
type ManagerState string

const (
	ManagerStateIdle         ManagerState = "idle"
	ManagerStateConnecting   ManagerState = "connecting"
	ManagerStateMuxReady     ManagerState = "mux_ready"
	ManagerStateReconnecting ManagerState = "reconnecting"
	ManagerStateFailed       ManagerState = "failed"
	ManagerStateClosed       ManagerState = "closed"
)

// ProfileConfig configures one carrier profile. Managers try profiles in order
// and move new streams to the next profile after a failed carrier setup.
type ProfileConfig = Config

// ManagerConfig configures a persistent carrier session. When Server is true,
// the manager builds a server-side smux session and AcceptStream receives
// streams from the peer. Otherwise OpenStream opens client-side streams.
type ManagerConfig struct {
	Session              Config
	Profiles             []ProfileConfig
	Server               bool
	MaxConcurrentStreams int
}

type managerSession interface {
	OpenStream(ctx context.Context) (net.Conn, error)
	AcceptStream(ctx context.Context) (net.Conn, error)
	SendDatagram(ctx context.Context, payload []byte) error
	SendDatagramTo(ctx context.Context, peerID string, payload []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)
	ReceivePeerDatagram(ctx context.Context) (transportapi.PeerDatagram, error)
	Close() error
	Capabilities() transportapi.Capabilities
}

// Manager keeps one olcRTC room/WebRTC carrier alive and multiplexes many
// higher-level streams over it.
type Manager struct {
	mu         sync.Mutex
	cfg        ManagerConfig
	state      ManagerState
	session    managerSession
	raw        net.Conn
	mux        *smux.Session
	profileIdx int
	streams    chan struct{}
	newSession func(context.Context, Config) (managerSession, error)
}

// NewManager creates a persistent carrier manager. The carrier is established
// lazily when OpenStream or AcceptStream is first called.
func NewManager(cfg ManagerConfig) *Manager {
	maxStreams := cfg.MaxConcurrentStreams
	if maxStreams == 0 {
		maxStreams = defaultMaxConcurrentStreams
	}
	var streams chan struct{}
	if maxStreams > 0 {
		streams = make(chan struct{}, maxStreams)
	}
	return &Manager{
		cfg:     cfg,
		state:   ManagerStateIdle,
		streams: streams,
		newSession: func(ctx context.Context, cfg Config) (managerSession, error) {
			return New(ctx, cfg)
		},
	}
}

// OpenStream opens one ordered reliable stream over the persistent carrier.
func (m *Manager) OpenStream(ctx context.Context) (net.Conn, error) {
	release, err := m.acquireStream(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	mux, err := m.readyMuxLocked(ctx)
	m.mu.Unlock()
	if err != nil {
		release()
		return nil, err
	}
	stream, err := mux.OpenStream()
	if err != nil {
		release()
		m.resetMux(mux)
		return nil, fmt.Errorf("olcrtc: open manager stream: %w", err)
	}
	return &managedConn{Conn: stream, release: release}, nil
}

// AcceptStream accepts one ordered reliable stream over the persistent carrier.
func (m *Manager) AcceptStream(ctx context.Context) (net.Conn, error) {
	release, err := m.acquireStream(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	mux, err := m.readyMuxLocked(ctx)
	m.mu.Unlock()
	if err != nil {
		release()
		return nil, err
	}

	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		stream, acceptErr := mux.AcceptStream()
		done <- result{conn: stream, err: acceptErr}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			release()
			m.resetMux(mux)
			return nil, fmt.Errorf("olcrtc: accept manager stream: %w", res.err)
		}
		return &managedConn{Conn: res.conn, release: release}, nil
	case <-ctx.Done():
		release()
		m.resetMux(mux)
		return nil, fmt.Errorf("olcrtc: accept manager stream context: %w", ctx.Err())
	}
}

// SendDatagram sends one unordered lossy datagram over the active carrier.
func (m *Manager) SendDatagram(ctx context.Context, payload []byte) error {
	return m.sendDatagram(ctx, "", payload)
}

// SendDatagramTo sends one unordered lossy datagram to a specific peer when the
// active carrier reports peer identities.
func (m *Manager) SendDatagramTo(ctx context.Context, peerID string, payload []byte) error {
	return m.sendDatagram(ctx, peerID, payload)
}

func (m *Manager) sendDatagram(ctx context.Context, peerID string, payload []byte) error {
	session, _, err := m.readyDatagramSession(ctx)
	if err != nil {
		return err
	}
	if peerID != "" {
		return session.SendDatagramTo(ctx, peerID, payload)
	}
	return session.SendDatagram(ctx, payload)
}

// ReceiveDatagram waits for one unordered lossy datagram over the active carrier.
func (m *Manager) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	packet, err := m.ReceivePeerDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return packet.Payload, nil
}

// ReceivePeerDatagram waits for one unordered lossy datagram and preserves peer
// identity when the carrier provides it.
func (m *Manager) ReceivePeerDatagram(ctx context.Context) (transportapi.PeerDatagram, error) {
	session, _, err := m.readyDatagramSession(ctx)
	if err != nil {
		return transportapi.PeerDatagram{}, err
	}
	return session.ReceivePeerDatagram(ctx)
}

// Capabilities reports the underlying session capabilities.
func (m *Manager) Capabilities() transportapi.Capabilities {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.session == nil {
		return transportapi.DefaultCapabilities()
	}
	return m.session.Capabilities()
}

// State returns the current manager lifecycle state.
func (m *Manager) State() ManagerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// ActiveProfile returns the profile currently backing the persistent carrier.
func (m *Manager) ActiveProfile() (int, Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.profileIdx, m.profileLocked(m.profileIdx)
}

// Close tears down the active mux, carrier connection, and olcRTC session.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeLocked(ManagerStateClosed)
}

func (m *Manager) readyMuxLocked(ctx context.Context) (*smux.Session, error) {
	if m.state == ManagerStateClosed {
		return nil, ErrManagerClosed
	}
	if m.mux != nil && !m.mux.IsClosed() {
		return m.mux, nil
	}
	if err := m.connectLocked(ctx); err != nil {
		m.state = ManagerStateFailed
		return nil, err
	}
	return m.mux, nil
}

func (m *Manager) readyDatagramSession(ctx context.Context) (managerSession, *smux.Session, error) {
	m.mu.Lock()
	mux, err := m.readyMuxLocked(ctx)
	session := m.session
	m.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	if session == nil {
		return nil, nil, ErrManagerClosed
	}
	return session, mux, nil
}

func (m *Manager) connectLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("olcrtc: manager context: %w", err)
	}
	if m.state == ManagerStateFailed {
		m.state = ManagerStateReconnecting
	} else {
		m.state = ManagerStateConnecting
	}

	profileCount := m.profileCountLocked()
	var lastErr error
	for attempt := 0; attempt < profileCount; attempt++ {
		idx := (m.profileIdx + attempt) % profileCount
		if err := m.connectProfileLocked(ctx, idx); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("olcrtc: no manager profiles configured")
	}
	return lastErr
}

func (m *Manager) connectProfileLocked(ctx context.Context, idx int) error {
	cfg := m.profileLocked(idx)
	sess, err := m.newSession(ctx, cfg)
	if err != nil {
		return fmt.Errorf("olcrtc: create manager session: %w", err)
	}

	var raw net.Conn
	if m.cfg.Server {
		raw, err = sess.AcceptStream(ctx)
	} else {
		raw, err = sess.OpenStream(ctx)
	}
	if err != nil {
		_ = sess.Close()
		return fmt.Errorf("olcrtc: open manager carrier: %w", err)
	}

	var mux *smux.Session
	if m.cfg.Server {
		mux, err = smux.Server(raw, smux.DefaultConfig())
	} else {
		mux, err = smux.Client(raw, smux.DefaultConfig())
	}
	if err != nil {
		_ = raw.Close()
		_ = sess.Close()
		return fmt.Errorf("olcrtc: create manager mux: %w", err)
	}

	m.session = sess
	m.raw = raw
	m.mux = mux
	m.profileIdx = idx
	m.state = ManagerStateMuxReady
	return nil
}

func (m *Manager) resetMux(dead *smux.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mux != dead {
		return
	}
	_ = m.closeLocked(ManagerStateIdle)
}

func (m *Manager) closeLocked(next ManagerState) error {
	var err error
	if m.mux != nil {
		err = m.mux.Close()
		m.mux = nil
	}
	if m.raw != nil {
		if closeErr := m.raw.Close(); err == nil {
			err = closeErr
		}
		m.raw = nil
	}
	if m.session != nil {
		if closeErr := m.session.Close(); err == nil {
			err = closeErr
		}
		m.session = nil
	}
	m.state = next
	return err
}

func (m *Manager) profileCountLocked() int {
	if len(m.cfg.Profiles) > 0 {
		return len(m.cfg.Profiles)
	}
	return 1
}

func (m *Manager) profileLocked(idx int) Config {
	if len(m.cfg.Profiles) == 0 {
		return m.cfg.Session
	}
	if idx < 0 || idx >= len(m.cfg.Profiles) {
		return m.cfg.Profiles[0]
	}
	return Config(m.cfg.Profiles[idx])
}

func (m *Manager) acquireStream(ctx context.Context) (func(), error) {
	if m.streams == nil {
		return func() {}, nil
	}
	select {
	case m.streams <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-m.streams })
		}, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrManagerStreamLimit, ctx.Err())
	}
}

type managedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *managedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
