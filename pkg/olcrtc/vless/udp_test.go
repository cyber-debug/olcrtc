package vless

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

var errUnexpectedUDPTarget = errors.New("unexpected udp target")

func TestUDPPacketRoundTrip(t *testing.T) {
	packet := UDPPacket{
		UserID:  vlessTestUserID(),
		Host:    testHostName,
		Port:    53,
		Payload: []byte("dns"),
	}
	wire, err := EncodeUDPPacket(packet)
	if err != nil {
		t.Fatalf("EncodeUDPPacket() error = %v", err)
	}
	got, err := DecodeUDPPacket(wire, map[uuid.UUID]struct{}{packet.UserID: {}})
	if err != nil {
		t.Fatalf("DecodeUDPPacket() error = %v", err)
	}
	if got.UserID != packet.UserID || got.Host != packet.Host || got.Port != packet.Port ||
		!bytes.Equal(got.Payload, packet.Payload) {
		t.Fatalf("DecodeUDPPacket() = %+v, want %+v", got, packet)
	}
}

func TestUDPPacketRejectsUnauthorizedUser(t *testing.T) {
	wire, err := EncodeUDPPacket(UDPPacket{
		UserID:  vlessTestUserID(),
		Host:    testHostName,
		Port:    53,
		Payload: []byte("dns"),
	})
	if err != nil {
		t.Fatalf("EncodeUDPPacket() error = %v", err)
	}
	_, err = DecodeUDPPacket(wire, map[uuid.UUID]struct{}{uuid.New(): {}})
	if !errors.Is(err, ErrUnauthorizedUser) {
		t.Fatalf("DecodeUDPPacket(unauthorized) error = %v, want %v", err, ErrUnauthorizedUser)
	}
}

func TestUDPPacketRejectsOversize(t *testing.T) {
	maxPayload, err := MaxUDPPayloadForHost(testHostName)
	if err != nil {
		t.Fatalf("MaxUDPPayloadForHost() error = %v", err)
	}
	_, err = EncodeUDPPacket(UDPPacket{
		UserID:  vlessTestUserID(),
		Host:    testHostName,
		Port:    53,
		Payload: make([]byte, maxPayload+1),
	})
	if !errors.Is(err, ErrUDPPacketTooLarge) {
		t.Fatalf("EncodeUDPPacket(oversize) error = %v, want %v", err, ErrUDPPacketTooLarge)
	}
}

func TestDecodeUDPPacketRejectsOversize(t *testing.T) {
	_, err := DecodeUDPPacket(make([]byte, MaxDatagramPayload+1), nil)
	if !errors.Is(err, ErrUDPPacketTooLarge) {
		t.Fatalf("DecodeUDPPacket(oversize) error = %v, want %v", err, ErrUDPPacketTooLarge)
	}
}

func TestServeUDPOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	userID := vlessTestUserID()
	serverIn := newMemoryDatagrams()
	clientIn := newMemoryDatagrams()

	if err := SendUDPPacket(ctx, serverIn, UDPPacket{
		UserID:  userID,
		Host:    testEchoHost,
		Port:    53,
		Payload: []byte("ping"),
	}); err != nil {
		t.Fatalf("SendUDPPacket() error = %v", err)
	}

	if err := ServeUDPOnce(ctx, serverIn, clientIn, map[uuid.UUID]struct{}{userID: {}}, echoUDPDial); err != nil {
		t.Fatalf("ServeUDPOnce() error = %v", err)
	}
	got, err := ReceiveUDPPacket(ctx, clientIn, map[uuid.UUID]struct{}{userID: {}})
	if err != nil {
		t.Fatalf("ReceiveUDPPacket() error = %v", err)
	}
	if got.Host != testEchoHost || got.Port != 53 || string(got.Payload) != "ping" {
		t.Fatalf("ReceiveUDPPacket() = %+v, want echo.local:53 ping", got)
	}
}

func TestServeUDPRelaysMultiplePacketsOverOneAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	userID := vlessTestUserID()
	serverIn := newMemoryPeerDatagrams(8)
	clientIn := newMemoryPeerDatagrams(8)
	stats := &UDPRelayStats{}
	var dials atomic.Uint64

	errCh := startUDPRelay(ctx, serverIn, clientIn, map[uuid.UUID]struct{}{userID: {}}, stats, func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		dials.Add(1)
		return echoUDPDial(ctx, network, address)
	})

	for _, payload := range []string{"one", "two"} {
		sendTestUDPPacket(ctx, t, serverIn, "", userID, []byte(payload))
		got := receiveTestUDPPacket(ctx, t, clientIn, userID)
		if string(got.Payload) != payload {
			t.Fatalf("UDP response payload = %q, want %q", got.Payload, payload)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("udp target dials = %d, want 1", got)
	}
	cancel()
	waitUDPRelayStopped(t, errCh)
	metrics := stats.Snapshot()
	if metrics.PacketsIn != 2 || metrics.PacketsOut != 2 || metrics.AssociationsOpened != 1 {
		t.Fatalf("UDP relay metrics = %+v, want 2 in, 2 out, 1 association", metrics)
	}
}

func TestServeUDPRelaysPeerDatagramResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	userID := vlessTestUserID()
	serverIn := newMemoryPeerDatagrams(4)
	clientIn := newMemoryPeerDatagrams(4)
	errCh := startUDPRelay(ctx, serverIn, clientIn, map[uuid.UUID]struct{}{userID: {}}, nil, echoUDPDial)

	sendTestUDPPacket(ctx, t, serverIn, "peer-a", userID, []byte("peer"))
	got := receiveTestPeerUDPPacket(ctx, t, clientIn, userID)
	if got.PeerID != "peer-a" || string(got.Packet.Payload) != "peer" {
		t.Fatalf("peer UDP response = %+v, want peer-a/peer", got)
	}
	cancel()
	waitUDPRelayStopped(t, errCh)
}

func TestServeUDPDropsWhenAssociationLimitReached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	userID := vlessTestUserID()
	serverIn := newMemoryPeerDatagrams(4)
	clientIn := newMemoryPeerDatagrams(4)
	stats := &UDPRelayStats{}
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeUDP(ctx, serverIn, clientIn, map[uuid.UUID]struct{}{userID: {}}, echoUDPDial, UDPRelayConfig{
			IdleTimeout:       time.Second,
			TargetReadTimeout: time.Second,
			MaxAssociations:   1,
			Stats:             stats,
		})
	}()

	sendTestUDPPacket(ctx, t, serverIn, "", userID, []byte("first"))
	_ = receiveTestUDPPacket(ctx, t, clientIn, userID)
	sendTestUDPPacketToHost(ctx, t, serverIn, "", userID, "second.local", []byte("second"))

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stats.Snapshot().AssociationLimitDrops == 1 {
			cancel()
			waitUDPRelayStopped(t, errCh)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("AssociationLimitDrops = %d, want 1", stats.Snapshot().AssociationLimitDrops)
}

type memoryDatagrams struct {
	ch chan []byte
}

func newMemoryDatagrams() *memoryDatagrams {
	return &memoryDatagrams{ch: make(chan []byte, 4)}
}

func (m *memoryDatagrams) SendDatagram(ctx context.Context, payload []byte) error {
	copied := make([]byte, len(payload))
	copy(copied, payload)
	select {
	case m.ch <- copied:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("send memory datagram: %w", ctx.Err())
	}
}

func (m *memoryDatagrams) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-m.ch:
		return payload, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("receive memory datagram: %w", ctx.Err())
	}
}

func echoUDPDial(_ context.Context, network, address string) (net.Conn, error) {
	if network != "udp" || address != testEchoHost+":53" {
		return nil, errUnexpectedUDPTarget
	}
	left, right := net.Pipe()
	go func() {
		defer func() { _ = right.Close() }()
		_, _ = io.Copy(right, right)
	}()
	return left, nil
}

type memoryPeerDatagrams struct {
	ch chan transportapi.PeerDatagram
}

func newMemoryPeerDatagrams(size int) *memoryPeerDatagrams {
	return &memoryPeerDatagrams{ch: make(chan transportapi.PeerDatagram, size)}
}

func (m *memoryPeerDatagrams) SendDatagram(ctx context.Context, payload []byte) error {
	return m.SendDatagramTo(ctx, "", payload)
}

func (m *memoryPeerDatagrams) SendDatagramTo(ctx context.Context, peerID string, payload []byte) error {
	copied := make([]byte, len(payload))
	copy(copied, payload)
	select {
	case m.ch <- transportapi.PeerDatagram{PeerID: peerID, Payload: copied}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("send memory peer datagram: %w", ctx.Err())
	}
}

func (m *memoryPeerDatagrams) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	dg, err := m.ReceivePeerDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return dg.Payload, nil
}

func (m *memoryPeerDatagrams) ReceivePeerDatagram(ctx context.Context) (transportapi.PeerDatagram, error) {
	select {
	case payload := <-m.ch:
		return payload, nil
	case <-ctx.Done():
		return transportapi.PeerDatagram{}, fmt.Errorf("receive memory peer datagram: %w", ctx.Err())
	}
}

func startUDPRelay(
	ctx context.Context,
	receiver transportapi.DatagramReceiver,
	sender transportapi.DatagramSender,
	allowed map[uuid.UUID]struct{},
	stats *UDPRelayStats,
	dial DialContext,
) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeUDP(ctx, receiver, sender, allowed, dial, UDPRelayConfig{
			IdleTimeout:       time.Second,
			TargetReadTimeout: 20 * time.Millisecond,
			MaxAssociations:   8,
			Stats:             stats,
		})
	}()
	return errCh
}

func sendTestUDPPacket(
	ctx context.Context,
	t *testing.T,
	sender transportapi.DatagramSender,
	peerID string,
	userID uuid.UUID,
	payload []byte,
) {
	t.Helper()
	sendTestUDPPacketToHost(ctx, t, sender, peerID, userID, testEchoHost, payload)
}

func sendTestUDPPacketToHost(
	ctx context.Context,
	t *testing.T,
	sender transportapi.DatagramSender,
	peerID string,
	userID uuid.UUID,
	host string,
	payload []byte,
) {
	t.Helper()
	if err := SendUDPPacketTo(ctx, sender, peerID, UDPPacket{
		UserID:  userID,
		Host:    host,
		Port:    53,
		Payload: payload,
	}); err != nil {
		t.Fatalf("SendUDPPacketTo() error = %v", err)
	}
}

func receiveTestUDPPacket(
	ctx context.Context,
	t *testing.T,
	receiver transportapi.DatagramReceiver,
	userID uuid.UUID,
) UDPPacket {
	t.Helper()
	got, err := ReceiveUDPPacket(ctx, receiver, map[uuid.UUID]struct{}{userID: {}})
	if err != nil {
		t.Fatalf("ReceiveUDPPacket() error = %v", err)
	}
	return got
}

func receiveTestPeerUDPPacket(
	ctx context.Context,
	t *testing.T,
	receiver transportapi.DatagramReceiver,
	userID uuid.UUID,
) PeerUDPPacket {
	t.Helper()
	got, err := ReceivePeerUDPPacket(ctx, receiver, map[uuid.UUID]struct{}{userID: {}})
	if err != nil {
		t.Fatalf("ReceivePeerUDPPacket() error = %v", err)
	}
	return got
}

func waitUDPRelayStopped(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("ServeUDP() error = nil, want context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("ServeUDP() did not stop")
	}
}
