package olcrtc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

type fakeManagerSession struct {
	open          net.Conn
	accept        net.Conn
	datagrams     chan transportapi.PeerDatagram
	closed        atomic.Bool
	mu            sync.Mutex
	sentDatagrams []transportapi.PeerDatagram
}

func (s *fakeManagerSession) OpenStream(context.Context) (net.Conn, error) {
	return s.open, nil
}

func (s *fakeManagerSession) AcceptStream(context.Context) (net.Conn, error) {
	return s.accept, nil
}

func (s *fakeManagerSession) SendDatagram(_ context.Context, payload []byte) error {
	return s.SendDatagramTo(context.Background(), "", payload)
}

func (s *fakeManagerSession) SendDatagramTo(_ context.Context, peerID string, payload []byte) error {
	packet := transportapi.PeerDatagram{PeerID: peerID, Payload: append([]byte(nil), payload...)}
	s.mu.Lock()
	s.sentDatagrams = append(s.sentDatagrams, packet)
	s.mu.Unlock()
	return nil
}

func (s *fakeManagerSession) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	packet, err := s.ReceivePeerDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return packet.Payload, nil
}

func (s *fakeManagerSession) ReceivePeerDatagram(ctx context.Context) (transportapi.PeerDatagram, error) {
	select {
	case packet := <-s.datagrams:
		return packet, nil
	case <-ctx.Done():
		return transportapi.PeerDatagram{}, ctx.Err()
	}
}

func (s *fakeManagerSession) Close() error {
	s.closed.Store(true)
	return nil
}

func (s *fakeManagerSession) Capabilities() transportapi.Capabilities {
	return transportapi.DefaultCapabilities()
}

func TestManagerReusesCarrierForStreams(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	var clientSessions atomic.Int32
	client := NewManager(ManagerConfig{})
	client.newSession = func(context.Context, Config) (managerSession, error) {
		clientSessions.Add(1)
		return &fakeManagerSession{open: left}, nil
	}
	defer client.Close()

	var serverSessions atomic.Int32
	server := NewManager(ManagerConfig{Server: true})
	server.newSession = func(context.Context, Config) (managerSession, error) {
		serverSessions.Add(1)
		return &fakeManagerSession{accept: right}, nil
	}
	defer server.Close()

	roundTrip := func(payload string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		accepted := make(chan net.Conn, 1)
		acceptErr := make(chan error, 1)
		go func() {
			conn, err := server.AcceptStream(ctx)
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- conn
		}()

		clientConn, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatalf("OpenStream() error = %v", err)
		}

		var serverConn net.Conn
		select {
		case err := <-acceptErr:
			t.Fatalf("AcceptStream() error = %v", err)
		case serverConn = <-accepted:
		case <-ctx.Done():
			t.Fatalf("AcceptStream() timed out: %v", ctx.Err())
		}

		if _, err := clientConn.Write([]byte(payload)); err != nil {
			t.Fatalf("client Write() error = %v", err)
		}
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(serverConn, buf); err != nil {
			t.Fatalf("server ReadFull() error = %v", err)
		}
		if got := string(buf); got != payload {
			t.Fatalf("server read %q, want %q", got, payload)
		}

		_ = clientConn.Close()
		_ = serverConn.Close()
	}

	roundTrip("first")
	roundTrip("second")

	if got := clientSessions.Load(); got != 1 {
		t.Fatalf("client session creations = %d, want 1", got)
	}
	if got := serverSessions.Load(); got != 1 {
		t.Fatalf("server session creations = %d, want 1", got)
	}
	if got := client.State(); got != ManagerStateMuxReady {
		t.Fatalf("client State() = %s, want %s", got, ManagerStateMuxReady)
	}
}

func TestManagerDatagramsUseActiveCarrier(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	fakeClient := &fakeManagerSession{open: left, datagrams: make(chan transportapi.PeerDatagram, 1)}
	client := NewManager(ManagerConfig{})
	client.newSession = func(context.Context, Config) (managerSession, error) {
		return fakeClient, nil
	}
	defer client.Close()

	fakeServer := &fakeManagerSession{accept: right, datagrams: make(chan transportapi.PeerDatagram, 1)}
	server := NewManager(ManagerConfig{Server: true})
	server.newSession = func(context.Context, Config) (managerSession, error) {
		return fakeServer, nil
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := server.AcceptStream(ctx)
		accepted <- conn
	}()
	clientConn, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	defer clientConn.Close()
	serverConn := <-accepted
	defer serverConn.Close()

	if err := client.SendDatagram(ctx, []byte("broadcast")); err != nil {
		t.Fatalf("SendDatagram() error = %v", err)
	}
	if err := server.SendDatagramTo(ctx, "peer-a", []byte("direct")); err != nil {
		t.Fatalf("SendDatagramTo() error = %v", err)
	}
	fakeClient.mu.Lock()
	clientSent := append([]transportapi.PeerDatagram(nil), fakeClient.sentDatagrams...)
	fakeClient.mu.Unlock()
	fakeServer.mu.Lock()
	serverSent := append([]transportapi.PeerDatagram(nil), fakeServer.sentDatagrams...)
	fakeServer.mu.Unlock()
	if len(clientSent) != 1 || clientSent[0].PeerID != "" || string(clientSent[0].Payload) != "broadcast" {
		t.Fatalf("client sent datagrams = %+v, want broadcast", clientSent)
	}
	if len(serverSent) != 1 || serverSent[0].PeerID != "peer-a" || string(serverSent[0].Payload) != "direct" {
		t.Fatalf("server sent datagrams = %+v, want peer-a/direct", serverSent)
	}

	fakeClient.datagrams <- transportapi.PeerDatagram{PeerID: "peer-b", Payload: []byte("reply")}
	got, err := client.ReceivePeerDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceivePeerDatagram() error = %v", err)
	}
	if got.PeerID != "peer-b" || string(got.Payload) != "reply" {
		t.Fatalf("ReceivePeerDatagram() = %+v, want peer-b/reply", got)
	}
}

func TestManagerFailsOverToNextProfile(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	client := NewManager(ManagerConfig{
		Profiles: []ProfileConfig{
			{Name: "broken"},
			{Name: "working"},
		},
	})
	var clientAttempts atomic.Int32
	client.newSession = func(_ context.Context, cfg Config) (managerSession, error) {
		clientAttempts.Add(1)
		if cfg.Name == "broken" {
			return nil, errors.New("profile unavailable")
		}
		return &fakeManagerSession{open: left}, nil
	}
	defer client.Close()

	server := NewManager(ManagerConfig{Server: true})
	server.newSession = func(context.Context, Config) (managerSession, error) {
		return &fakeManagerSession{accept: right}, nil
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := server.AcceptStream(ctx)
		accepted <- conn
	}()

	clientConn, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	serverConn := <-accepted
	defer clientConn.Close()
	defer serverConn.Close()

	if got := clientAttempts.Load(); got != 2 {
		t.Fatalf("client session attempts = %d, want 2", got)
	}
	idx, profile := client.ActiveProfile()
	if idx != 1 || profile.Name != "working" {
		t.Fatalf("ActiveProfile() = (%d, %q), want (1, working)", idx, profile.Name)
	}
}

func TestManagerEnforcesMaxConcurrentStreams(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	client := NewManager(ManagerConfig{MaxConcurrentStreams: 1})
	client.newSession = func(context.Context, Config) (managerSession, error) {
		return &fakeManagerSession{open: left}, nil
	}
	defer client.Close()

	server := NewManager(ManagerConfig{Server: true})
	server.newSession = func(context.Context, Config) (managerSession, error) {
		return &fakeManagerSession{accept: right}, nil
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := server.AcceptStream(ctx)
		accepted <- conn
	}()

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("first OpenStream() error = %v", err)
	}
	defer first.Close()
	serverConn := <-accepted
	defer serverConn.Close()

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	_, err = client.OpenStream(shortCtx)
	if !errors.Is(err, ErrManagerStreamLimit) {
		t.Fatalf("second OpenStream() error = %v, want %v", err, ErrManagerStreamLimit)
	}
}
