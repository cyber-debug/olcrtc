package olcrtc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
	"github.com/pion/webrtc/v4"
)

const (
	stubToken = "tok"
	stubURL   = "wss://x/"
)

// --- stub engine ---

type stubSession struct {
	connected       bool
	onEnded         func(string)
	watchBlock      chan struct{} // closed to unblock WatchConnection
	caps            engine.Capabilities
	datagramCanSend bool
	sentDatagrams   [][]byte
	sentPeerID      string
}

func newStubSession() *stubSession { return &stubSession{watchBlock: make(chan struct{})} }

func (s *stubSession) Connect(_ context.Context) error                  { s.connected = true; return nil }
func (s *stubSession) Send(_ []byte) error                              { return nil }
func (s *stubSession) Close() error                                     { return nil }
func (s *stubSession) SetReconnectCallback(_ func(*webrtc.DataChannel)) {}
func (s *stubSession) SetShouldReconnect(_ func() bool)                 {}
func (s *stubSession) SetEndedCallback(cb func(string))                 { s.onEnded = cb }
func (s *stubSession) WatchConnection(_ context.Context)                { <-s.watchBlock }
func (s *stubSession) CanSend() bool                                    { return s.connected }
func (s *stubSession) GetSendQueue() chan []byte                        { return nil }
func (s *stubSession) GetBufferedAmount() uint64                        { return 0 }
func (s *stubSession) Reconnect(_ string)                               {}
func (s *stubSession) Capabilities() engine.Capabilities {
	if s.caps != (engine.Capabilities{}) {
		return s.caps
	}
	return engine.Capabilities{ByteStream: true}
}
func (s *stubSession) SubscriberCanSend() bool { return s.connected }
func (s *stubSession) SendDatagram(data []byte) error {
	copied := make([]byte, len(data))
	copy(copied, data)
	s.sentDatagrams = append(s.sentDatagrams, copied)
	return nil
}
func (s *stubSession) SendDatagramTo(peerID string, data []byte) error {
	s.sentPeerID = peerID
	return s.SendDatagram(data)
}
func (s *stubSession) DatagramCanSend() bool { return s.datagramCanSend }

// Compile-time check: stubSession must satisfy engine.Session.
var _ engine.Session = (*stubSession)(nil)
var _ transportapi.Dialer = (*olcrtc.Session)(nil)
var _ transportapi.Listener = (*olcrtc.Session)(nil)
var _ transportapi.DatagramSender = (*olcrtc.Session)(nil)
var _ transportapi.DatagramReceiver = (*olcrtc.Session)(nil)
var _ transportapi.PeerDatagramSender = (*olcrtc.Session)(nil)
var _ transportapi.PeerDatagramReceiver = (*olcrtc.Session)(nil)

func registerStubEngine(t *testing.T, name string) {
	t.Helper()
	engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
		return newStubSession(), nil
	})
	t.Cleanup(func() {
		engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
			return newStubSession(), nil
		})
	})
}

// registerStubEngineControlled registers an engine that returns a pre-built stub the test controls.
func registerStubEngineControlled(t *testing.T, name string, stub *stubSession) {
	t.Helper()
	engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
		return stub, nil
	})
	t.Cleanup(func() {
		engine.Register(name, func(_ context.Context, _ engine.Config) (engine.Session, error) {
			return newStubSession(), nil
		})
	})
}

// --- stub auth ---

type stubAuth struct{ engineName string }

func (a stubAuth) Engine() string          { return a.engineName }
func (stubAuth) DefaultServiceURL() string { return "https://stub.example" }
func (a stubAuth) Issue(_ context.Context, cfg auth.Config) (auth.Credentials, error) {
	if cfg.RoomURL == "" {
		return auth.Credentials{}, auth.ErrRoomIDRequired
	}
	return auth.Credentials{URL: "wss://stub/", Token: stubToken}, nil
}

type stubAuthWithRoomCreator struct{ stubAuth }

func (stubAuthWithRoomCreator) CreateRoom(_ context.Context, _ auth.Config) (string, error) {
	return "created-room-id", nil
}

func registerStubAuth(t *testing.T, name, engineName string) {
	t.Helper()
	auth.Register(name, stubAuth{engineName: engineName})
}

func registerStubAuthWithCreator(t *testing.T, name, engineName string) {
	t.Helper()
	auth.Register(name, stubAuthWithRoomCreator{stubAuth{engineName: engineName}})
}

// --- tests ---

func TestNewDirect_MissingURL(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{Token: "tok"})
	if !errors.Is(err, olcrtc.ErrURLRequired) {
		t.Fatalf("New(no url) = %v, want ErrURLRequired", err)
	}
}

func TestNewDirect_MissingToken(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{URL: stubURL})
	if !errors.Is(err, olcrtc.ErrTokenRequired) {
		t.Fatalf("New(no token) = %v, want ErrTokenRequired", err)
	}
}

func TestNewDirect_UnknownEngine(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "no-such-engine",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err == nil {
		t.Fatal("New(bad engine) error = nil")
	}
}

func TestNewDirect_OK(t *testing.T) {
	registerStubEngine(t, "stub-direct")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-direct",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !sess.CanSend() {
		t.Fatal("CanSend() = false after connect")
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewAuth_UnknownProvider(t *testing.T) {
	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth:   "no-such-auth",
		RoomID: "room",
	})
	if err == nil {
		t.Fatal("New(bad auth) error = nil")
	}
}

func TestNewAuth_MissingRoomID(t *testing.T) {
	registerStubEngine(t, "stub-auth-engine")
	registerStubAuth(t, "stub-auth-noroomid", "stub-auth-engine")

	_, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth: "stub-auth-noroomid",
		// RoomID intentionally empty
	})
	if err == nil {
		t.Fatal("New(auth, no room) error = nil")
	}
}

func TestNewAuth_OK(t *testing.T) {
	registerStubEngine(t, "stub-auth-ok-engine")
	registerStubAuth(t, "stub-auth-ok", "stub-auth-ok-engine")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Auth:   "stub-auth-ok",
		RoomID: "some-room",
	})
	if err != nil {
		t.Fatalf("New(auth) error = %v", err)
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	_ = sess.Close()
}

func TestRegisterDefaults_Idempotent(_ *testing.T) {
	olcrtc.RegisterDefaults()
	olcrtc.RegisterDefaults()
}

func TestCreateRoom_Unsupported(t *testing.T) {
	registerStubAuth(t, "stub-nocreate", "stub-direct")

	_, err := olcrtc.CreateRoom(context.Background(), "stub-nocreate")
	if !errors.Is(err, olcrtc.ErrRoomCreationUnsupported) {
		t.Fatalf("CreateRoom(no creator) = %v, want ErrRoomCreationUnsupported", err)
	}
}

func TestCreateRoom_OK(t *testing.T) {
	registerStubEngine(t, "stub-creator-engine")
	registerStubAuthWithCreator(t, "stub-creator", "stub-creator-engine")

	roomID, err := olcrtc.CreateRoom(context.Background(), "stub-creator")
	if err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	if roomID == "" {
		t.Fatal("CreateRoom() returned empty room ID")
	}
}

func TestDial_ReadUnblocksOnSessionEnd(t *testing.T) {
	stub := newStubSession()
	registerStubEngineControlled(t, "stub-ended", stub)

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-ended",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	c, err := sess.Dial(context.Background())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 4)
		_, err := c.Read(buf)
		readErr <- err
	}()

	// Simulate session ending permanently.
	stub.onEnded("test reason")
	close(stub.watchBlock)

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("Read() should return error after session ended")
		}
	case <-time.After(time.Second):
		t.Fatal("Read() did not unblock after session ended")
	}
}

func TestSessionCapabilities(t *testing.T) {
	stub := newStubSession()
	registerStubEngineControlled(t, "stub-caps", stub)

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-caps",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	caps := sess.Capabilities()
	if caps.Version != transportapi.ProtocolVersion || !caps.ReliableStreams || !caps.OrderedStreams {
		t.Fatalf("Capabilities() = %+v", caps)
	}
	if caps.LossyDatagrams {
		t.Fatalf("Capabilities().LossyDatagrams = true, want false for stub")
	}
}

func TestSessionDatagramSendReceive(t *testing.T) {
	stub := newStubSession()
	stub.caps = engine.Capabilities{ByteStream: true, Datagram: true}
	stub.datagramCanSend = true
	var engineCfg engine.Config
	engine.Register("stub-datagram", func(_ context.Context, cfg engine.Config) (engine.Session, error) {
		engineCfg = cfg
		return stub, nil
	})

	var callbackPayload []byte
	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine:         "stub-datagram",
		URL:            stubURL,
		Token:          stubToken,
		DatagramBuffer: 1,
		OnDatagram: func(data []byte) {
			callbackPayload = data
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := sess.SendDatagram(context.Background(), []byte("out")); err != nil {
		t.Fatalf("SendDatagram() error = %v", err)
	}
	if len(stub.sentDatagrams) != 1 || string(stub.sentDatagrams[0]) != "out" {
		t.Fatalf("sent datagrams = %q", stub.sentDatagrams)
	}

	raw := []byte("in")
	engineCfg.OnDatagram(raw)
	raw[0] = 'X'
	got, err := sess.ReceiveDatagram(context.Background())
	if err != nil {
		t.Fatalf("ReceiveDatagram() error = %v", err)
	}
	if string(got) != "in" {
		t.Fatalf("ReceiveDatagram() = %q, want in", got)
	}
	if string(callbackPayload) != "in" {
		t.Fatalf("OnDatagram payload = %q, want in", callbackPayload)
	}
	metrics := sess.Metrics()
	if metrics.DatagramsIn != 1 || metrics.DatagramsOut != 1 {
		t.Fatalf("Metrics() datagrams = in:%d out:%d, want 1/1", metrics.DatagramsIn, metrics.DatagramsOut)
	}
}

func TestSessionPeerDatagramSendReceive(t *testing.T) {
	stub := newStubSession()
	stub.caps = engine.Capabilities{ByteStream: true, Datagram: true}
	stub.datagramCanSend = true
	var engineCfg engine.Config
	engine.Register("stub-peer-datagram", func(_ context.Context, cfg engine.Config) (engine.Session, error) {
		engineCfg = cfg
		return stub, nil
	})

	var callbackPeer string
	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine:         "stub-peer-datagram",
		URL:            stubURL,
		Token:          stubToken,
		DatagramBuffer: 1,
		OnPeerDatagram: func(peerID string, _ []byte) {
			callbackPeer = peerID
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := sess.SendDatagramTo(context.Background(), "peer-a", []byte("out")); err != nil {
		t.Fatalf("SendDatagramTo() error = %v", err)
	}
	if stub.sentPeerID != "peer-a" || len(stub.sentDatagrams) != 1 || string(stub.sentDatagrams[0]) != "out" {
		t.Fatalf("peer send = peer:%q payload:%q", stub.sentPeerID, stub.sentDatagrams)
	}

	engineCfg.OnPeerDatagram("peer-b", []byte("in"))
	got, err := sess.ReceivePeerDatagram(context.Background())
	if err != nil {
		t.Fatalf("ReceivePeerDatagram() error = %v", err)
	}
	if got.PeerID != "peer-b" || string(got.Payload) != "in" {
		t.Fatalf("ReceivePeerDatagram() = %+v, want peer-b/in", got)
	}
	if callbackPeer != "peer-b" {
		t.Fatalf("OnPeerDatagram peer = %q, want peer-b", callbackPeer)
	}
}

func TestSessionDatagramRejectsOversize(t *testing.T) {
	stub := newStubSession()
	stub.caps = engine.Capabilities{ByteStream: true, Datagram: true}
	stub.datagramCanSend = true
	registerStubEngineControlled(t, "stub-datagram-oversize", stub)

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-datagram-oversize",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	payload := make([]byte, transportapi.MaxDatagramPayload+1)
	if err := sess.SendDatagram(context.Background(), payload); !errors.Is(err, olcrtc.ErrDatagramTooLarge) {
		t.Fatalf("SendDatagram(oversize) error = %v, want %v", err, olcrtc.ErrDatagramTooLarge)
	}
}

func TestSessionDatagramUnsupportedWhenNotSendable(t *testing.T) {
	stub := newStubSession()
	registerStubEngineControlled(t, "stub-datagram-unsupported", stub)

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-datagram-unsupported",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := sess.SendDatagram(context.Background(), []byte("x")); !errors.Is(err, engine.ErrDatagramUnsupported) {
		t.Fatalf("SendDatagram(unsupported) error = %v, want %v", err, engine.ErrDatagramUnsupported)
	}
}

func TestSessionDatagramQueueDropsWhenFull(t *testing.T) {
	var engineCfg engine.Config
	engine.Register("stub-datagram-drop", func(_ context.Context, cfg engine.Config) (engine.Session, error) {
		engineCfg = cfg
		return newStubSession(), nil
	})

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine:         "stub-datagram-drop",
		URL:            stubURL,
		Token:          stubToken,
		DatagramBuffer: 1,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	engineCfg.OnDatagram([]byte("first"))
	engineCfg.OnDatagram([]byte("second"))
	got, err := sess.ReceiveDatagram(context.Background())
	if err != nil {
		t.Fatalf("ReceiveDatagram(first) error = %v", err)
	}
	if string(got) != "first" {
		t.Fatalf("ReceiveDatagram(first) = %q, want first", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got, err := sess.ReceiveDatagram(ctx); err == nil {
		t.Fatalf("ReceiveDatagram(second) = %q, want timeout after drop", got)
	}
	if drops := sess.Metrics().DatagramDrops; drops != 1 {
		t.Fatalf("Metrics().DatagramDrops = %d, want 1", drops)
	}
}

func TestSessionDatagramReceiveUnblocksOnClose(t *testing.T) {
	stub := newStubSession()
	registerStubEngineControlled(t, "stub-datagram-close", stub)

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-datagram-close",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := sess.ReceiveDatagram(context.Background())
		done <- err
	}()
	if err := sess.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, olcrtc.ErrSessionEnded) {
			t.Fatalf("ReceiveDatagram(after close) error = %v, want %v", err, olcrtc.ErrSessionEnded)
		}
	case <-time.After(time.Second):
		t.Fatal("ReceiveDatagram() did not unblock after Close")
	}
}

func TestAcceptStreamOpensSingleSessionStream(t *testing.T) {
	registerStubEngine(t, "stub-accept")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-accept",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	c, err := sess.AcceptStream(context.Background())
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	if got := sess.Metrics().OpenedStreams; got != 1 {
		t.Fatalf("Metrics().OpenedStreams = %d, want 1", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := sess.Metrics().ClosedStreams; got != 1 {
		t.Fatalf("Metrics().ClosedStreams = %d, want 1", got)
	}
}

func TestDial_RoundTrip(t *testing.T) {
	registerStubEngine(t, "stub-dial")

	sess, err := olcrtc.New(context.Background(), olcrtc.Config{
		Engine: "stub-dial",
		URL:    stubURL,
		Token:  stubToken,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	c, err := sess.Dial(context.Background())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}

	// Write should succeed (stub Send is a no-op).
	payload := []byte("hello")
	n, err := c.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write() = (%d, %v)", n, err)
	}

	// Close should unblock any pending Read.
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Read after close should return an error (pipe closed).
	buf := make([]byte, 4)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("Read() after Close() should return error")
	}
}
