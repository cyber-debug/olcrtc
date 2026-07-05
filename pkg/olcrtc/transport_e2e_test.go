package olcrtc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/vless"
	"github.com/pion/webrtc/v4"
)

var (
	errMemoryStreamPayloadMismatch = errors.New("memory stream payload mismatch")
	errUnexpectedMemoryUDPTarget   = errors.New("unexpected memory udp target")
)

const memoryTestEchoHost = "echo.local"

func TestSessionTransportOverMemoryEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	engineName := registerMemoryEngine(t)

	server, err := olcrtc.New(ctx, olcrtc.Config{Engine: engineName, URL: stubURL, Token: stubToken})
	if err != nil {
		t.Fatalf("New(server) error = %v", err)
	}
	client, err := olcrtc.New(ctx, olcrtc.Config{Engine: engineName, URL: stubURL, Token: stubToken})
	if err != nil {
		t.Fatalf("New(client) error = %v", err)
	}

	serverConn, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	defer func() { _ = serverConn.Close() }()
	clientConn, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	assertMemoryStreamPayload(t, clientConn, serverConn)
	assertMemoryPeerDatagrams(ctx, t, client, server)
}

func TestSessionVLESSUDPRelayOverMemoryEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	engineName := registerMemoryEngine(t)
	userID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	server, err := olcrtc.New(ctx, olcrtc.Config{Engine: engineName, URL: stubURL, Token: stubToken})
	if err != nil {
		t.Fatalf("New(server) error = %v", err)
	}
	defer func() { _ = server.Close() }()
	client, err := olcrtc.New(ctx, olcrtc.Config{Engine: engineName, URL: stubURL, Token: stubToken})
	if err != nil {
		t.Fatalf("New(client) error = %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := server.Connect(ctx); err != nil {
		t.Fatalf("Connect(server) error = %v", err)
	}
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect(client) error = %v", err)
	}

	relayCancel, errCh := startMemoryVLESSUDPRelay(ctx, server, userID)
	defer relayCancel()
	assertMemoryVLESSUDPRelay(ctx, t, client, userID)
	stopMemoryVLESSUDPRelay(t, relayCancel, errCh)
}

func assertMemoryStreamPayload(t *testing.T, clientConn, serverConn net.Conn) {
	t.Helper()
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, len("stream"))
		if _, err := io.ReadFull(serverConn, buf); err != nil {
			readDone <- fmt.Errorf("read stream: %w", err)
			return
		}
		if string(buf) != "stream" {
			readDone <- fmt.Errorf("%w: got %q want stream", errMemoryStreamPayloadMismatch, buf)
			return
		}
		readDone <- nil
	}()
	if _, err := clientConn.Write([]byte("stream")); err != nil {
		t.Fatalf("write stream: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
}

func assertMemoryPeerDatagrams(ctx context.Context, t *testing.T, client, server *olcrtc.Session) {
	t.Helper()
	if err := client.SendDatagram(ctx, []byte("udp")); err != nil {
		t.Fatalf("SendDatagram() error = %v", err)
	}
	got, err := server.ReceivePeerDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceivePeerDatagram(server) error = %v", err)
	}
	if got.PeerID == "" || string(got.Payload) != "udp" {
		t.Fatalf("server peer datagram = %+v, want peer/udp", got)
	}
	if err := server.SendDatagramTo(ctx, got.PeerID, []byte("pong")); err != nil {
		t.Fatalf("SendDatagramTo() error = %v", err)
	}
	reply, err := client.ReceivePeerDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceivePeerDatagram(client) error = %v", err)
	}
	if string(reply.Payload) != "pong" {
		t.Fatalf("client peer datagram = %+v, want pong", reply)
	}
}

type memoryEngineRoom struct {
	mu       sync.Mutex
	nextPeer int
	sessions map[string]*memoryEngineSession
}

func registerMemoryEngine(t *testing.T) string {
	t.Helper()
	name := "pkg-memory-" + t.Name()
	room := &memoryEngineRoom{sessions: make(map[string]*memoryEngineSession)}
	engine.Register(name, func(_ context.Context, cfg engine.Config) (engine.Session, error) {
		room.mu.Lock()
		room.nextPeer++
		peerID := fmt.Sprintf("peer-%d", room.nextPeer)
		sess := &memoryEngineSession{
			room:           room,
			peerID:         peerID,
			onData:         cfg.OnData,
			onDatagram:     cfg.OnDatagram,
			onPeerDatagram: cfg.OnPeerDatagram,
		}
		room.sessions[peerID] = sess
		room.mu.Unlock()
		return sess, nil
	})
	return name
}

type memoryEngineSession struct {
	room           *memoryEngineRoom
	peerID         string
	onData         func([]byte)
	onDatagram     func([]byte)
	onPeerDatagram func(peerID string, data []byte)
	ended          func(string)
	mu             sync.Mutex
	connected      bool
	closed         bool
}

func (s *memoryEngineSession) Connect(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = true
	return nil
}

func (s *memoryEngineSession) Send(data []byte) error {
	for _, peer := range s.peers() {
		peer.deliverData(data)
	}
	return nil
}

func (s *memoryEngineSession) SendDatagram(data []byte) error {
	for _, peer := range s.peers() {
		peer.deliverDatagram(s.peerID, data)
	}
	return nil
}

func (s *memoryEngineSession) SendDatagramTo(peerID string, data []byte) error {
	s.room.mu.Lock()
	peer := s.room.sessions[peerID]
	s.room.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("memory engine peer %q: %w", peerID, net.ErrClosed)
	}
	peer.deliverDatagram(s.peerID, data)
	return nil
}

func (s *memoryEngineSession) DatagramCanSend() bool { return s.isConnected() }

func (s *memoryEngineSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.connected = false
	s.mu.Unlock()
	s.room.mu.Lock()
	delete(s.room.sessions, s.peerID)
	s.room.mu.Unlock()
	return nil
}

func (s *memoryEngineSession) SetReconnectCallback(func(*webrtc.DataChannel)) {}
func (s *memoryEngineSession) SetShouldReconnect(func() bool)                 {}
func (s *memoryEngineSession) SetEndedCallback(cb func(string)) {
	s.mu.Lock()
	s.ended = cb
	s.mu.Unlock()
}
func (s *memoryEngineSession) WatchConnection(ctx context.Context) { <-ctx.Done() }
func (s *memoryEngineSession) CanSend() bool                       { return s.isConnected() }
func (s *memoryEngineSession) SubscriberCanSend() bool             { return s.isConnected() }
func (s *memoryEngineSession) GetSendQueue() chan []byte           { return nil }
func (s *memoryEngineSession) GetBufferedAmount() uint64           { return 0 }
func (s *memoryEngineSession) Reconnect(string)                    {}
func (s *memoryEngineSession) Capabilities() engine.Capabilities {
	return engine.Capabilities{ByteStream: true, Datagram: true}
}

func (s *memoryEngineSession) peers() []*memoryEngineSession {
	s.room.mu.Lock()
	defer s.room.mu.Unlock()
	peers := make([]*memoryEngineSession, 0, len(s.room.sessions))
	for peerID, peer := range s.room.sessions {
		if peerID != s.peerID {
			peers = append(peers, peer)
		}
	}
	return peers
}

func (s *memoryEngineSession) deliverData(data []byte) {
	if !s.isConnected() || s.onData == nil {
		return
	}
	payload := append([]byte(nil), data...)
	s.onData(payload)
}

func (s *memoryEngineSession) deliverDatagram(peerID string, data []byte) {
	if !s.isConnected() {
		return
	}
	payload := append([]byte(nil), data...)
	if s.onPeerDatagram != nil {
		s.onPeerDatagram(peerID, payload)
		return
	}
	if s.onDatagram != nil {
		s.onDatagram(payload)
	}
}

func (s *memoryEngineSession) isConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected && !s.closed
}

func memoryEchoUDPDial(_ context.Context, network, address string) (net.Conn, error) {
	if network != "udp" || address != memoryTestEchoHost+":53" {
		return nil, fmt.Errorf("%w: %s/%s", errUnexpectedMemoryUDPTarget, network, address)
	}
	left, right := net.Pipe()
	go func() {
		defer func() { _ = right.Close() }()
		_, _ = io.Copy(right, right)
	}()
	return left, nil
}

func startMemoryVLESSUDPRelay(
	ctx context.Context,
	server *olcrtc.Session,
	userID uuid.UUID,
) (context.CancelFunc, <-chan error) {
	relayCtx, relayCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- vless.ServeUDP(
			relayCtx,
			server,
			server,
			map[uuid.UUID]struct{}{userID: {}},
			memoryEchoUDPDial,
			vless.UDPRelayConfig{TargetReadTimeout: 20 * time.Millisecond},
		)
	}()
	return relayCancel, errCh
}

func assertMemoryVLESSUDPRelay(ctx context.Context, t *testing.T, client *olcrtc.Session, userID uuid.UUID) {
	t.Helper()
	if err := vless.SendUDPPacket(ctx, client, vless.UDPPacket{
		UserID:  userID,
		Host:    memoryTestEchoHost,
		Port:    53,
		Payload: []byte("relay"),
	}); err != nil {
		t.Fatalf("SendUDPPacket() error = %v", err)
	}
	got, err := vless.ReceiveUDPPacket(ctx, client, map[uuid.UUID]struct{}{userID: {}})
	if err != nil {
		t.Fatalf("ReceiveUDPPacket() error = %v", err)
	}
	if got.Host != memoryTestEchoHost || got.Port != 53 || string(got.Payload) != "relay" {
		t.Fatalf("VLESS UDP relay response = %+v, want echo.local:53 relay", got)
	}
}

func stopMemoryVLESSUDPRelay(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("ServeUDP() error = nil, want context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("ServeUDP() did not stop")
	}
}

var (
	_ engine.Session             = (*memoryEngineSession)(nil)
	_ engine.DatagramSession     = (*memoryEngineSession)(nil)
	_ engine.PeerDatagramSession = (*memoryEngineSession)(nil)
)
