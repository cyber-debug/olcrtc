// Package olcrtc exposes olcrtc as an embeddable Go library.
//
// Typical usage - obtain a [net.Conn]-compatible handle and dial:
//
//	sess, err := olcrtc.New(ctx, olcrtc.Config{
//	    Engine: "livekit",
//	    URL:    "wss://sfu.example/",
//	    Token:  "<livekit-jwt>",
//	})
//	if err != nil { ... }
//	conn, err := sess.Dial(ctx)  // blocks until WebRTC data channel is ready
//	// conn implements net.Conn - pass it to sing-box / any io.ReadWriter consumer
//
// Built-in auth providers (jitsi, telemost, wbstream):
//
//	sess, err := olcrtc.New(ctx, olcrtc.Config{
//	    Auth:   "jitsi",
//	    // Use meet.small-dm.ru, meet1.arbitr.ru, or meet.handyweb.org - whichever works in your network
//	    RoomID: "https://meet.small-dm.ru/myroom",
//	})
//
// Import the implementations you need via blank imports, or call [RegisterDefaults]:
//
//	import (
//	    _ "github.com/openlibrecommunity/olcrtc/internal/engine/jitsi"
//	    _ "github.com/openlibrecommunity/olcrtc/internal/auth/jitsi"
//	)
package olcrtc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

var (
	// ErrURLRequired is returned when direct mode is used without a URL.
	ErrURLRequired = errors.New("olcrtc: URL required when using direct engine mode")
	// ErrTokenRequired is returned when direct mode is used without a token.
	ErrTokenRequired = errors.New("olcrtc: Token required when using direct engine mode")
	// ErrRoomCreationUnsupported is returned when the auth provider cannot create rooms.
	ErrRoomCreationUnsupported = errors.New("olcrtc: auth provider does not support room creation")
	// ErrSessionEnded is returned from Read/Write when the session has ended permanently.
	ErrSessionEnded = errors.New("olcrtc: session ended")
	// ErrDatagramTooLarge is returned when a lossy datagram exceeds the public transport contract.
	ErrDatagramTooLarge = errors.New("olcrtc: datagram too large")
)

const defaultDatagramBuffer = 64

// Config is the input to [New].
type Config struct {
	// --- built-in auth mode ---
	// Auth is the name of a registered auth provider ("jitsi", "telemost", "wbstream").
	// When set, RoomID is forwarded to the provider as the room reference.
	Auth   string
	RoomID string

	// --- direct engine mode (Auth == "") ---
	// Engine selects the SFU protocol ("livekit", "goolom", "jitsi").
	// Defaults to "livekit" when Auth is empty.
	Engine string
	URL    string
	Token  string

	// --- common ---
	// Name is the display name used when joining the room.
	Name string
	// DNSServer is an optional custom DNS resolver (e.g. "8.8.8.8:53").
	DNSServer string
	// ProxyAddr / ProxyPort configure an outbound SOCKS5 proxy.
	ProxyAddr string
	ProxyPort int

	// DatagramBuffer is the inbound lossy datagram queue size. Values <= 0 use
	// a small default. When the queue is full, new datagrams are dropped.
	DatagramBuffer int
	// OnDatagram is called for each inbound unordered/lossy datagram.
	OnDatagram func([]byte)
	// OnPeerDatagram is called when an engine reports the sending peer.
	OnPeerDatagram func(peerID string, data []byte)
}

// Session is the library handle returned by [New].
// Call [Session.Dial] to connect and obtain a [net.Conn].
type Session struct {
	inner        engine.Session
	pr           *io.PipeReader
	pw           *io.PipeWriter
	authProvider auth.Provider
	authCfg      auth.Config
	datagrams    chan transportapi.PeerDatagram
	done         chan struct{}
	closeOnce    sync.Once
	counters     *transportapi.Counters
}

// RegisterDefaults registers all built-in engines and auth providers.
// Call once at program start if you want the full set without manual blank
// imports. Safe to call multiple times.
func RegisterDefaults() {
	enginebuiltin.RegisterDefaults()
}

// New creates a Session from cfg. The session is not connected yet; call
// [Session.Connect] when ready.
func New(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Auth != "" {
		return newWithAuth(ctx, cfg)
	}
	return newDirect(ctx, cfg)
}

func newWithAuth(ctx context.Context, cfg Config) (*Session, error) {
	p, err := auth.Get(cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("olcrtc: auth provider %q not registered: %w", cfg.Auth, err)
	}

	authCfg := auth.Config{
		RoomURL:   cfg.RoomID,
		Name:      cfg.Name,
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
	}

	creds, err := p.Issue(ctx, authCfg)
	if err != nil {
		return nil, fmt.Errorf("olcrtc: auth issue: %w", err)
	}

	pr, pw := io.Pipe()
	datagrams := make(chan transportapi.PeerDatagram, datagramBufferSize(cfg.DatagramBuffer))
	done := make(chan struct{})
	counters := &transportapi.Counters{}
	engineName := p.Engine()
	sess, err := engine.New(ctx, engineName, engine.Config{
		URL:        creds.URL,
		Token:      creds.Token,
		Name:       cfg.Name,
		Extra:      creds.Extra,
		OnData:     func(data []byte) { _, _ = pw.Write(data) },
		OnDatagram: func(data []byte) { enqueueDatagram(done, datagrams, counters, cfg.OnDatagram, data) },
		OnPeerDatagram: func(peerID string, data []byte) {
			enqueuePeerDatagram(done, datagrams, counters, cfg.OnPeerDatagram, peerID, data)
		},
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
		Refresh: func(rCtx context.Context) (engine.Credentials, error) {
			fresh, freshErr := p.Issue(rCtx, authCfg)
			if freshErr != nil {
				return engine.Credentials{}, fmt.Errorf("olcrtc: auth refresh: %w", freshErr)
			}
			return engine.Credentials{URL: fresh.URL, Token: fresh.Token, Extra: fresh.Extra}, nil
		},
	})
	if err != nil {
		_ = pw.CloseWithError(err)
		return nil, fmt.Errorf("olcrtc: engine %q: %w", engineName, err)
	}

	return &Session{
		inner:        sess,
		pr:           pr,
		pw:           pw,
		authProvider: p,
		authCfg:      authCfg,
		datagrams:    datagrams,
		done:         done,
		counters:     counters,
	}, nil
}

func newDirect(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	if cfg.Token == "" {
		return nil, ErrTokenRequired
	}

	engineName := cfg.Engine
	if engineName == "" {
		engineName = "livekit"
	}

	pr, pw := io.Pipe()
	datagrams := make(chan transportapi.PeerDatagram, datagramBufferSize(cfg.DatagramBuffer))
	done := make(chan struct{})
	counters := &transportapi.Counters{}
	sess, err := engine.New(ctx, engineName, engine.Config{
		URL:        cfg.URL,
		Token:      cfg.Token,
		Name:       cfg.Name,
		OnData:     func(data []byte) { _, _ = pw.Write(data) },
		OnDatagram: func(data []byte) { enqueueDatagram(done, datagrams, counters, cfg.OnDatagram, data) },
		OnPeerDatagram: func(peerID string, data []byte) {
			enqueuePeerDatagram(done, datagrams, counters, cfg.OnPeerDatagram, peerID, data)
		},
		DNSServer: cfg.DNSServer,
		ProxyAddr: cfg.ProxyAddr,
		ProxyPort: cfg.ProxyPort,
	})
	if err != nil {
		_ = pw.CloseWithError(err)
		return nil, fmt.Errorf("olcrtc: engine %q: %w", engineName, err)
	}

	return &Session{inner: sess, pr: pr, pw: pw, datagrams: datagrams, done: done, counters: counters}, nil
}

// Dial connects and returns a [net.Conn] backed by the WebRTC data channel.
// It combines [Session.Connect] + wrapping in a single call.
// The connection watcher runs in the background for the lifetime of ctx;
// when the session ends permanently, Read will return an error.
func (s *Session) Dial(ctx context.Context) (net.Conn, error) {
	s.inner.SetEndedCallback(func(_ string) {
		s.endSession(ErrSessionEnded)
	})
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}
	go s.inner.WatchConnection(ctx)
	s.counters.StreamOpened()
	return &conn{s: s}, nil
}

// OpenStream opens one reliable ordered byte stream. It is equivalent to
// [Session.Dial] and exists so Session satisfies transportapi.Dialer.
func (s *Session) OpenStream(ctx context.Context) (net.Conn, error) {
	return s.Dial(ctx)
}

// AcceptStream accepts the single reliable ordered byte stream exposed by the
// current olcrtc session. It is equivalent to [Session.Dial]; olcrtc does not
// expose independent multi-accept streams through this package yet.
func (s *Session) AcceptStream(ctx context.Context) (net.Conn, error) {
	return s.Dial(ctx)
}

// Capabilities reports the public transport contract exposed by Session.
func (s *Session) Capabilities() transportapi.Capabilities {
	caps := transportapi.DefaultCapabilities()
	if s == nil || s.inner == nil {
		return caps
	}
	engineCaps := s.inner.Capabilities()
	caps.LossyDatagrams = engineCaps.Datagram
	return caps
}

// SendDatagram sends one unordered/lossy datagram when the selected engine
// supports a datagram path.
func (s *Session) SendDatagram(ctx context.Context, payload []byte) error {
	return s.sendDatagram(ctx, "", payload)
}

// SendDatagramTo sends one unordered/lossy datagram to peerID when the
// selected engine can address peers. An empty peerID falls back to broadcast
// SendDatagram semantics.
func (s *Session) SendDatagramTo(ctx context.Context, peerID string, payload []byte) error {
	return s.sendDatagram(ctx, peerID, payload)
}

func (s *Session) sendDatagram(ctx context.Context, peerID string, payload []byte) error {
	if len(payload) > transportapi.MaxDatagramPayload {
		return ErrDatagramTooLarge
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("send datagram context: %w", err)
	}
	if peerID != "" {
		return s.sendPeerDatagram(peerID, payload)
	}
	return s.sendBroadcastDatagram(payload)
}

func (s *Session) sendPeerDatagram(peerID string, payload []byte) error {
	peer, ok := s.inner.(engine.PeerDatagramSession)
	if !ok {
		return engine.ErrDatagramUnsupported
	}
	if dg, ok := s.inner.(engine.DatagramSession); ok && !dg.DatagramCanSend() {
		return engine.ErrDatagramUnsupported
	}
	if err := peer.SendDatagramTo(peerID, payload); err != nil {
		return fmt.Errorf("send peer datagram: %w", err)
	}
	s.counters.DatagramOut()
	return nil
}

func (s *Session) sendBroadcastDatagram(payload []byte) error {
	dg, ok := s.inner.(engine.DatagramSession)
	if !ok {
		return engine.ErrDatagramUnsupported
	}
	if !dg.DatagramCanSend() {
		return engine.ErrDatagramUnsupported
	}
	if err := dg.SendDatagram(payload); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	s.counters.DatagramOut()
	return nil
}

// ReceiveDatagram waits for one inbound unordered/lossy datagram.
func (s *Session) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	dg, err := s.ReceivePeerDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return dg.Payload, nil
}

// ReceivePeerDatagram waits for one inbound unordered/lossy datagram and
// returns peer identity when the engine reported one.
func (s *Session) ReceivePeerDatagram(ctx context.Context) (transportapi.PeerDatagram, error) {
	select {
	case dg := <-s.datagrams:
		return dg, nil
	case <-s.done:
		return transportapi.PeerDatagram{}, ErrSessionEnded
	case <-ctx.Done():
		return transportapi.PeerDatagram{}, fmt.Errorf("receive datagram context: %w", ctx.Err())
	}
}

// Metrics returns an immutable snapshot of process-local transport counters.
func (s *Session) Metrics() transportapi.Metrics {
	return s.counters.Snapshot()
}

// Connect establishes the WebRTC connection. Blocks until the data channel (or
// media) is ready, or ctx is cancelled.
func (s *Session) Connect(ctx context.Context) error {
	if err := s.inner.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	return nil
}

// Send queues data for transmission over the data channel.
func (s *Session) Send(data []byte) error {
	if err := s.inner.Send(data); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

func datagramBufferSize(size int) int {
	if size > 0 {
		return size
	}
	return defaultDatagramBuffer
}

func enqueuePeerDatagram(
	done <-chan struct{},
	queue chan<- transportapi.PeerDatagram,
	counters *transportapi.Counters,
	cb func(peerID string, data []byte),
	peerID string,
	data []byte,
) {
	if cb != nil {
		cb(peerID, cloneBytes(data))
	}
	enqueueDatagramPacket(done, queue, counters, transportapi.PeerDatagram{
		PeerID:  peerID,
		Payload: cloneBytes(data),
	})
}

func enqueueDatagram(
	done <-chan struct{},
	queue chan<- transportapi.PeerDatagram,
	counters *transportapi.Counters,
	cb func([]byte),
	data []byte,
) {
	payload := cloneBytes(data)
	if cb != nil {
		cb(cloneBytes(payload))
	}
	enqueueDatagramPacket(done, queue, counters, transportapi.PeerDatagram{Payload: payload})
}

func enqueueDatagramPacket(
	done <-chan struct{},
	queue chan<- transportapi.PeerDatagram,
	counters *transportapi.Counters,
	dg transportapi.PeerDatagram,
) {
	select {
	case <-done:
		if counters != nil {
			counters.DatagramDrop()
		}
	case queue <- dg:
		if counters != nil {
			counters.DatagramIn()
		}
	default:
		if counters != nil {
			counters.DatagramDrop()
		}
	}
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

// Close tears down the session and releases all resources.
func (s *Session) Close() error {
	s.endSession(net.ErrClosed)
	if err := s.inner.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

func (s *Session) endSession(err error) {
	s.closeOnce.Do(func() {
		_ = s.pw.CloseWithError(err)
		close(s.done)
	})
}

// WatchConnection monitors the connection and handles reconnects. Run in a
// goroutine alongside Connect.
func (s *Session) WatchConnection(ctx context.Context) {
	s.inner.WatchConnection(ctx)
}

// CanSend reports whether the session is ready to accept outgoing data.
func (s *Session) CanSend() bool {
	return s.inner.CanSend()
}

// SetEndedCallback registers a function called when the session ends
// permanently (after reconnect exhaustion or explicit close).
func (s *Session) SetEndedCallback(cb func(reason string)) {
	s.inner.SetEndedCallback(cb)
}

// SetShouldReconnect controls whether automatic reconnection is attempted.
func (s *Session) SetShouldReconnect(fn func() bool) {
	s.inner.SetShouldReconnect(fn)
}

// CreateRoom creates a new room via the auth provider and returns the room ID.
// Only works when Auth names a provider that supports room creation. Built-in
// providers currently return [ErrRoomCreationUnsupported].
func CreateRoom(ctx context.Context, authName string) (string, error) {
	p, err := auth.Get(authName)
	if err != nil {
		return "", fmt.Errorf("olcrtc: auth provider %q not registered: %w", authName, err)
	}
	creator, ok := p.(auth.RoomCreator)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRoomCreationUnsupported, authName)
	}
	roomID, err := creator.CreateRoom(ctx, auth.Config{})
	if err != nil {
		return "", fmt.Errorf("olcrtc: create room: %w", err)
	}
	return roomID, nil
}
