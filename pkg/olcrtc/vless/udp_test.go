package vless

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
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
