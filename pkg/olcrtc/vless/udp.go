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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

const (
	udpRelayTimeout        = 5 * time.Second
	defaultUDPIdleTimeout  = 30 * time.Second
	defaultUDPReadTimeout  = time.Second
	defaultUDPAssociations = 256
)

var (
	// ErrInvalidUDPPacket is returned when a VLESS UDP datagram is malformed.
	ErrInvalidUDPPacket = errors.New("vless: invalid udp packet")
	// ErrUDPPacketTooLarge is returned when an encoded UDP datagram exceeds the transport contract.
	ErrUDPPacketTooLarge = errors.New("vless: udp packet too large")
	// ErrUDPAssociationLimit is returned when the UDP relay association table is full.
	ErrUDPAssociationLimit = errors.New("vless: udp association limit reached")
)

// UDPPacket is one authenticated VLESS UDP payload for a target endpoint.
type UDPPacket struct {
	UserID  uuid.UUID
	Host    string
	Port    uint16
	Payload []byte
}

// UDPRelayConfig tunes a long-running VLESS UDP relay.
type UDPRelayConfig struct {
	IdleTimeout       time.Duration
	TargetReadTimeout time.Duration
	MaxAssociations   int
	Stats             *UDPRelayStats
}

// UDPRelayMetrics is a point-in-time VLESS UDP relay counter snapshot.
type UDPRelayMetrics struct {
	PacketsIn             uint64
	PacketsOut            uint64
	AssociationsOpened    uint64
	AssociationsClosed    uint64
	AssociationLimitDrops uint64
	DecodeErrors          uint64
	DialErrors            uint64
	TargetErrors          uint64
	SendErrors            uint64
	ActiveAssociations    int64
}

// UDPRelayStats stores process-local VLESS UDP relay counters.
type UDPRelayStats struct {
	packetsIn             atomic.Uint64
	packetsOut            atomic.Uint64
	associationsOpened    atomic.Uint64
	associationsClosed    atomic.Uint64
	associationLimitDrops atomic.Uint64
	decodeErrors          atomic.Uint64
	dialErrors            atomic.Uint64
	targetErrors          atomic.Uint64
	sendErrors            atomic.Uint64
	activeAssociations    atomic.Int64
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
	return SendUDPPacketTo(ctx, sender, "", p)
}

// SendUDPPacketTo encodes and sends one VLESS UDP packet to peerID when the
// transport supports peer-addressed datagrams.
func SendUDPPacketTo(
	ctx context.Context,
	sender transportapi.DatagramSender,
	peerID string,
	p UDPPacket,
) error {
	wire, err := EncodeUDPPacket(p)
	if err != nil {
		return err
	}
	if peerID != "" {
		if peer, ok := sender.(transportapi.PeerDatagramSender); ok {
			if err := peer.SendDatagramTo(ctx, peerID, wire); err != nil {
				return fmt.Errorf("send vless udp packet to peer: %w", err)
			}
			return nil
		}
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
	packet, err := ReceivePeerUDPPacket(ctx, receiver, allowed)
	return packet.Packet, err
}

// PeerUDPPacket is one decoded VLESS UDP packet with optional transport peer identity.
type PeerUDPPacket struct {
	PeerID string
	Packet UDPPacket
}

// ReceivePeerUDPPacket receives and decodes one VLESS UDP packet, preserving
// the transport peer identity when the receiver exposes one.
func ReceivePeerUDPPacket(
	ctx context.Context,
	receiver transportapi.DatagramReceiver,
	allowed map[uuid.UUID]struct{},
) (PeerUDPPacket, error) {
	var (
		peerID string
		wire   []byte
		err    error
	)
	if peer, ok := receiver.(transportapi.PeerDatagramReceiver); ok {
		dg, recvErr := peer.ReceivePeerDatagram(ctx)
		peerID = dg.PeerID
		wire = dg.Payload
		err = recvErr
	} else {
		wire, err = receiver.ReceiveDatagram(ctx)
	}
	if err != nil {
		return PeerUDPPacket{}, fmt.Errorf("receive vless udp packet: %w", err)
	}
	packet, err := DecodeUDPPacket(wire, allowed)
	if err != nil {
		return PeerUDPPacket{}, err
	}
	return PeerUDPPacket{PeerID: peerID, Packet: packet}, nil
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

// ServeUDP relays VLESS UDP datagrams until ctx is cancelled. It keeps one UDP
// target connection per user, peer and destination tuple, then expires idle
// associations automatically.
func ServeUDP(
	ctx context.Context,
	receiver transportapi.DatagramReceiver,
	sender transportapi.DatagramSender,
	allowed map[uuid.UUID]struct{},
	dial DialContext,
	cfg UDPRelayConfig,
) error {
	relay := newUDPRelay(sender, allowed, dial, cfg)
	defer relay.close()
	return relay.run(ctx, receiver)
}

// Snapshot returns an immutable relay counter view.
func (s *UDPRelayStats) Snapshot() UDPRelayMetrics {
	if s == nil {
		return UDPRelayMetrics{}
	}
	return UDPRelayMetrics{
		PacketsIn:             s.packetsIn.Load(),
		PacketsOut:            s.packetsOut.Load(),
		AssociationsOpened:    s.associationsOpened.Load(),
		AssociationsClosed:    s.associationsClosed.Load(),
		AssociationLimitDrops: s.associationLimitDrops.Load(),
		DecodeErrors:          s.decodeErrors.Load(),
		DialErrors:            s.dialErrors.Load(),
		TargetErrors:          s.targetErrors.Load(),
		SendErrors:            s.sendErrors.Load(),
		ActiveAssociations:    s.activeAssociations.Load(),
	}
}

type udpRelay struct {
	sender  transportapi.DatagramSender
	allowed map[uuid.UUID]struct{}
	dial    DialContext
	cfg     UDPRelayConfig
	mu      sync.Mutex
	assoc   map[udpAssociationKey]*udpAssociation
}

type udpAssociationKey struct {
	peerID string
	userID uuid.UUID
	host   string
	port   uint16
}

type udpAssociation struct {
	relay      *udpRelay
	key        udpAssociationKey
	conn       net.Conn
	maxPayload int
	lastActive atomic.Int64
}

func newUDPRelay(
	sender transportapi.DatagramSender,
	allowed map[uuid.UUID]struct{},
	dial DialContext,
	cfg UDPRelayConfig,
) *udpRelay {
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultUDPIdleTimeout
	}
	if cfg.TargetReadTimeout <= 0 {
		cfg.TargetReadTimeout = defaultUDPReadTimeout
	}
	if cfg.MaxAssociations <= 0 {
		cfg.MaxAssociations = defaultUDPAssociations
	}
	if cfg.Stats == nil {
		cfg.Stats = &UDPRelayStats{}
	}
	return &udpRelay{
		sender:  sender,
		allowed: allowed,
		dial:    dial,
		cfg:     cfg,
		assoc:   make(map[udpAssociationKey]*udpAssociation),
	}
}

func (r *udpRelay) run(ctx context.Context, receiver transportapi.DatagramReceiver) error {
	for {
		p, err := ReceivePeerUDPPacket(ctx, receiver, r.allowed)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("vless udp relay context: %w", ctx.Err())
			}
			r.cfg.Stats.decodeErrors.Add(1)
			continue
		}
		r.cfg.Stats.packetsIn.Add(1)
		if err := r.forward(ctx, p); err != nil && ctx.Err() != nil {
			return fmt.Errorf("vless udp relay context: %w", ctx.Err())
		}
	}
}

func (r *udpRelay) forward(ctx context.Context, p PeerUDPPacket) error {
	a, err := r.getAssociation(ctx, p)
	if err != nil {
		r.recordAssociationError(err)
		return err
	}
	a.touch()
	if _, err := a.conn.Write(p.Packet.Payload); err != nil {
		r.cfg.Stats.targetErrors.Add(1)
		return fmt.Errorf("write vless udp target: %w", err)
	}
	return nil
}

func (r *udpRelay) getAssociation(ctx context.Context, p PeerUDPPacket) (*udpAssociation, error) {
	key := udpAssociationKey{
		peerID: p.PeerID,
		userID: p.Packet.UserID,
		host:   p.Packet.Host,
		port:   p.Packet.Port,
	}
	if a := r.lookupAssociation(key); a != nil {
		return a, nil
	}
	return r.openAssociation(ctx, key)
}

func (r *udpRelay) lookupAssociation(key udpAssociationKey) *udpAssociation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.assoc[key]
}

func (r *udpRelay) openAssociation(ctx context.Context, key udpAssociationKey) (*udpAssociation, error) {
	if err := r.reserveAssociationSlot(); err != nil {
		return nil, err
	}
	conn, err := r.dial(ctx, "udp", key.target())
	if err != nil {
		return nil, fmt.Errorf("dial vless udp target %s: %w", key.target(), err)
	}
	maxPayload, err := MaxUDPPayloadForHost(key.host)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	a := &udpAssociation{relay: r, key: key, conn: conn, maxPayload: maxPayload}
	a.touch()
	existing, installed, err := r.installAssociation(a)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if existing != nil {
		_ = conn.Close()
		return existing, nil
	}
	if installed {
		go a.readLoop(ctx)
	}
	return a, nil
}

func (r *udpRelay) reserveAssociationSlot() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.assoc) >= r.cfg.MaxAssociations {
		return ErrUDPAssociationLimit
	}
	return nil
}

func (r *udpRelay) installAssociation(a *udpAssociation) (*udpAssociation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.assoc[a.key]; existing != nil {
		return existing, false, nil
	}
	if len(r.assoc) >= r.cfg.MaxAssociations {
		return nil, false, ErrUDPAssociationLimit
	}
	r.assoc[a.key] = a
	r.cfg.Stats.associationsOpened.Add(1)
	r.cfg.Stats.activeAssociations.Add(1)
	return nil, true, nil
}

func (r *udpRelay) recordAssociationError(err error) {
	switch {
	case errors.Is(err, ErrUDPAssociationLimit):
		r.cfg.Stats.associationLimitDrops.Add(1)
	case err != nil:
		r.cfg.Stats.dialErrors.Add(1)
	}
}

func (r *udpRelay) removeAssociation(key udpAssociationKey, a *udpAssociation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.assoc[key] != a {
		return
	}
	delete(r.assoc, key)
	r.cfg.Stats.associationsClosed.Add(1)
	r.cfg.Stats.activeAssociations.Add(-1)
}

func (r *udpRelay) close() {
	r.mu.Lock()
	associations := make([]*udpAssociation, 0, len(r.assoc))
	for _, a := range r.assoc {
		associations = append(associations, a)
	}
	r.assoc = make(map[udpAssociationKey]*udpAssociation)
	r.cfg.Stats.activeAssociations.Store(0)
	r.mu.Unlock()
	for _, a := range associations {
		_ = a.conn.Close()
	}
}

func (a *udpAssociation) readLoop(ctx context.Context) {
	defer func() {
		a.relay.removeAssociation(a.key, a)
		_ = a.conn.Close()
	}()
	buf := make([]byte, a.maxPayload)
	for {
		_ = a.conn.SetReadDeadline(time.Now().Add(a.relay.cfg.TargetReadTimeout))
		n, err := a.conn.Read(buf)
		if n > 0 {
			a.sendResponse(ctx, buf[:n])
		}
		if err == nil {
			continue
		}
		if !a.keepReading(err) {
			return
		}
	}
}

func (a *udpAssociation) sendResponse(parentCtx context.Context, payload []byte) {
	ctx, cancel := context.WithTimeout(parentCtx, a.relay.cfg.TargetReadTimeout)
	defer cancel()
	err := SendUDPPacketTo(ctx, a.relay.sender, a.key.peerID, UDPPacket{
		UserID:  a.key.userID,
		Host:    a.key.host,
		Port:    a.key.port,
		Payload: payload,
	})
	if err != nil {
		a.relay.cfg.Stats.sendErrors.Add(1)
		return
	}
	a.relay.cfg.Stats.packetsOut.Add(1)
}

func (a *udpAssociation) keepReading(err error) bool {
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		a.relay.cfg.Stats.targetErrors.Add(1)
		return false
	}
	return time.Since(a.lastActiveTime()) < a.relay.cfg.IdleTimeout
}

func (a *udpAssociation) touch() {
	a.lastActive.Store(time.Now().UTC().UnixNano())
}

func (a *udpAssociation) lastActiveTime() time.Time {
	return time.Unix(0, a.lastActive.Load()).UTC()
}

func (k udpAssociationKey) target() string {
	return net.JoinHostPort(k.host, strconv.Itoa(int(k.port)))
}
