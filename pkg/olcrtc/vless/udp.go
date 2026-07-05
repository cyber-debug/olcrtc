package vless

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

const udpRelayTimeout = 5 * time.Second

var (
	// ErrInvalidUDPPacket is returned when a VLESS UDP datagram is malformed.
	ErrInvalidUDPPacket = errors.New("vless: invalid udp packet")
	// ErrUDPPacketTooLarge is returned when an encoded UDP datagram exceeds the transport contract.
	ErrUDPPacketTooLarge = errors.New("vless: udp packet too large")
)

// UDPPacket is one authenticated VLESS UDP payload for a target endpoint.
type UDPPacket struct {
	UserID  uuid.UUID
	Host    string
	Port    uint16
	Payload []byte
}

// MaxUDPPayloadForHost returns the maximum UDP payload bytes available for host
// while keeping the encoded packet inside MaxDatagramPayload.
func MaxUDPPayloadForHost(host string) (int, error) {
	_, addr, err := encodeAddr(host)
	if err != nil {
		return 0, err
	}
	overhead := 1 + 16 + 2 + 1 + len(addr)
	return MaxDatagramPayload - overhead, nil
}

// EncodeUDPPacket serializes one VLESS UDP packet for unordered/lossy transport delivery.
func EncodeUDPPacket(p UDPPacket) ([]byte, error) {
	if p.UserID == uuid.Nil {
		return nil, ErrUnauthorizedUser
	}
	if p.Port == 0 {
		return nil, ErrInvalidAddress
	}
	addrType, addr, err := encodeAddr(p.Host)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 20+len(addr)+len(p.Payload))
	out = append(out, Version)
	out = append(out, p.UserID[:]...)
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], p.Port)
	out = append(out, port[:]...)
	out = append(out, addrType)
	out = append(out, addr...)
	out = append(out, p.Payload...)
	if len(out) > MaxDatagramPayload {
		return nil, ErrUDPPacketTooLarge
	}
	return out, nil
}

// DecodeUDPPacket parses and authorizes one VLESS UDP packet.
func DecodeUDPPacket(data []byte, allowed map[uuid.UUID]struct{}) (UDPPacket, error) {
	if len(data) > MaxDatagramPayload {
		return UDPPacket{}, ErrUDPPacketTooLarge
	}
	if len(data) < 20 {
		return UDPPacket{}, ErrInvalidUDPPacket
	}
	if data[0] != Version {
		return UDPPacket{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, data[0])
	}
	userID, err := readUserID(data[1:17])
	if err != nil {
		return UDPPacket{}, err
	}
	if err := checkAllowedUser(userID, allowed); err != nil {
		return UDPPacket{}, err
	}
	p := UDPPacket{
		UserID: userID,
		Port:   binary.BigEndian.Uint16(data[17:19]),
	}
	if p.Port == 0 {
		return UDPPacket{}, ErrInvalidAddress
	}
	r := bytes.NewReader(data[19:])
	host, err := readAddr(r)
	if err != nil {
		return UDPPacket{}, err
	}
	p.Host = host
	p.Payload, err = io.ReadAll(r)
	if err != nil {
		return UDPPacket{}, fmt.Errorf("read vless udp payload: %w", err)
	}
	return p, nil
}

// SendUDPPacket encodes and sends one VLESS UDP packet over a lossy transport.
func SendUDPPacket(ctx context.Context, sender transportapi.DatagramSender, p UDPPacket) error {
	wire, err := EncodeUDPPacket(p)
	if err != nil {
		return err
	}
	if err := sender.SendDatagram(ctx, wire); err != nil {
		return fmt.Errorf("send vless udp packet: %w", err)
	}
	return nil
}

// ReceiveUDPPacket receives and decodes one VLESS UDP packet from a lossy transport.
func ReceiveUDPPacket(
	ctx context.Context,
	receiver transportapi.DatagramReceiver,
	allowed map[uuid.UUID]struct{},
) (UDPPacket, error) {
	wire, err := receiver.ReceiveDatagram(ctx)
	if err != nil {
		return UDPPacket{}, fmt.Errorf("receive vless udp packet: %w", err)
	}
	return DecodeUDPPacket(wire, allowed)
}

// ServeUDPOnce relays one inbound VLESS UDP packet and sends one UDP response.
func ServeUDPOnce(
	ctx context.Context,
	receiver transportapi.DatagramReceiver,
	sender transportapi.DatagramSender,
	allowed map[uuid.UUID]struct{},
	dial DialContext,
) error {
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	req, err := ReceiveUDPPacket(ctx, receiver, allowed)
	if err != nil {
		return err
	}
	target, err := dial(ctx, "udp", net.JoinHostPort(req.Host, strconv.Itoa(int(req.Port))))
	if err != nil {
		return fmt.Errorf("dial vless udp target %s:%d: %w", req.Host, req.Port, err)
	}
	defer func() { _ = target.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = target.SetDeadline(deadline)
	} else {
		_ = target.SetDeadline(time.Now().Add(udpRelayTimeout))
	}
	if _, err := target.Write(req.Payload); err != nil {
		return fmt.Errorf("write vless udp target: %w", err)
	}
	maxPayload, err := MaxUDPPayloadForHost(req.Host)
	if err != nil {
		return err
	}
	buf := make([]byte, maxPayload)
	n, err := target.Read(buf)
	if err != nil {
		return fmt.Errorf("read vless udp target: %w", err)
	}
	return SendUDPPacket(ctx, sender, UDPPacket{
		UserID:  req.UserID,
		Host:    req.Host,
		Port:    req.Port,
		Payload: buf[:n],
	})
}
