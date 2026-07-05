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

// ManagerState describes the lifecycle stage of a persistent olcRTC carrier.
type ManagerState string

const (
	ManagerStateIdle       ManagerState = "idle"
	ManagerStateConnecting ManagerState = "connecting"
	ManagerStateMuxReady   ManagerState = "mux_ready"
	ManagerStateFailed     ManagerState = "failed"
	ManagerStateClosed     ManagerState = "closed"
)

// ManagerConfig configures a persistent carrier session. When Server is true,
// the manager builds a server-side smux session and AcceptStream receives
// streams from the peer. Otherwise OpenStream opens client-side streams.
type ManagerConfig struct {
	Session Config
	Server  bool
}

type managerSession interface {
	OpenStream(ctx context.Context) (net.Conn, error)
	AcceptStream(ctx context.Context) (net.Conn, error)
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
	newSession func(context.Context, Config) (managerSession, error)
}

// NewManager creates a persistent carrier manager. The carrier is established
// lazily when OpenStream or AcceptStream is first called.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		cfg:   cfg,
		state: ManagerStateIdle,
		newSession: func(ctx context.Context, cfg Config) (managerSession, error) {
			return New(ctx, cfg)
		},
	}
}

// OpenStream opens one ordered reliable stream over the persistent carrier.
func (m *Manager) OpenStream(ctx context.Context) (net.Conn, error) {
	m.mu.Lock()
	mux, err := m.readyMuxLocked(ctx)
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	stream, err := mux.OpenStream()
	if err != nil {
		m.resetMux(mux)
		return nil, fmt.Errorf("olcrtc: open manager stream: %w", err)
	}
	return stream, nil
}

// AcceptStream accepts one ordered reliable stream over the persistent carrier.
func (m *Manager) AcceptStream(ctx context.Context) (net.Conn, error) {
	m.mu.Lock()
	mux, err := m.readyMuxLocked(ctx)
	m.mu.Unlock()
	if err != nil {
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
			m.resetMux(mux)
			return nil, fmt.Errorf("olcrtc: accept manager stream: %w", res.err)
		}
		return res.conn, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("olcrtc: accept manager stream context: %w", ctx.Err())
	}
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

func (m *Manager) connectLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("olcrtc: manager context: %w", err)
	}
	m.state = ManagerStateConnecting
	sess, err := m.newSession(ctx, m.cfg.Session)
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
