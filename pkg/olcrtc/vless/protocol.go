// Package vless implements the minimal VLESS TCP framing needed to run VLESS
// over an arbitrary reliable olcrtc transport stream.
package vless

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"

	"github.com/google/uuid"
)

const (
	// Version is the minimal VLESS protocol version supported by this adapter.
	Version = 0

	// CommandTCP is the VLESS TCP command.
	CommandTCP = 1
	// CommandUDP is the VLESS UDP command.
	CommandUDP = 2

	// AddrIPv4 identifies an IPv4 target address.
	AddrIPv4 = 1
	// AddrDomain identifies a domain-name target address.
	AddrDomain = 2
	// AddrIPv6 identifies an IPv6 target address.
	AddrIPv6 = 3

	// MaxDomainLen is the maximum VLESS domain target length.
	MaxDomainLen = 255
	// MaxDatagramPayload is the UDP payload size this adapter reserves for future datagram support.
	MaxDatagramPayload = 1200
)

var (
	// ErrUnsupportedVersion is returned when a peer sends an unsupported VLESS version.
	ErrUnsupportedVersion = errors.New("vless: unsupported version")

	// ErrUnauthorizedUser is returned when the VLESS user id is missing or not allowed.
	ErrUnauthorizedUser = errors.New("vless: unauthorized user")

	// ErrUnsupportedCommand is returned when the request command is not supported by this adapter.
	ErrUnsupportedCommand = errors.New("vless: unsupported command")

	// ErrInvalidAddress is returned when a request target address cannot be encoded or decoded.
	ErrInvalidAddress = errors.New("vless: invalid address")

	// ErrUDPUnsupported is returned for VLESS UDP requests until datagram plumbing is implemented.
	ErrUDPUnsupported = errors.New("vless: udp adapter not implemented")
)

// Request is the VLESS request header.
type Request struct {
	UserID  uuid.UUID
	Command byte
	Host    string
	Port    uint16
}

// Target returns host:port.
func (r Request) Target() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(int(r.Port)))
}

// WriteRequest writes a minimal VLESS request header.
func WriteRequest(w io.Writer, r Request) error {
	if r.UserID == uuid.Nil {
		return ErrUnauthorizedUser
	}
	if r.Command != CommandTCP && r.Command != CommandUDP {
		return fmt.Errorf("%w: %d", ErrUnsupportedCommand, r.Command)
	}
	addrType, addr, err := encodeAddr(r.Host)
	if err != nil {
		return err
	}
	header := make([]byte, 0, 22+len(addr))
	header = append(header, Version)
	header = append(header, r.UserID[:]...)
	header = append(header, 0) // opt length
	header = append(header, r.Command)
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], r.Port)
	header = append(header, port[:]...)
	header = append(header, addrType)
	header = append(header, addr...)
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write vless request: %w", err)
	}
	return nil
}

// ReadRequest reads and validates a minimal VLESS request header.
func ReadRequest(r io.Reader, allowed map[uuid.UUID]struct{}) (Request, error) {
	var fixed [21]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Request{}, fmt.Errorf("read vless request: %w", err)
	}
	if fixed[0] != Version {
		return Request{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, fixed[0])
	}
	userID, err := readUserID(fixed[1:17])
	if err != nil {
		return Request{}, err
	}
	if err := checkAllowedUser(userID, allowed); err != nil {
		return Request{}, err
	}
	if err := discardOptions(r, int(fixed[17])); err != nil {
		return Request{}, err
	}
	req := Request{
		UserID:  userID,
		Command: fixed[18],
		Port:    binary.BigEndian.Uint16(fixed[19:21]),
	}
	host, err := readAddr(r)
	if err != nil {
		return Request{}, err
	}
	req.Host = host
	if req.Command != CommandTCP && req.Command != CommandUDP {
		return Request{}, fmt.Errorf("%w: %d", ErrUnsupportedCommand, req.Command)
	}
	return req, nil
}

func readUserID(raw []byte) (uuid.UUID, error) {
	userID, err := uuid.FromBytes(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse vless user id: %w", err)
	}
	return userID, nil
}

func checkAllowedUser(userID uuid.UUID, allowed map[uuid.UUID]struct{}) error {
	if len(allowed) == 0 {
		return nil
	}
	if _, ok := allowed[userID]; !ok {
		return ErrUnauthorizedUser
	}
	return nil
}

func discardOptions(r io.Reader, optLen int) error {
	if optLen == 0 {
		return nil
	}
	if _, err := io.CopyN(io.Discard, r, int64(optLen)); err != nil {
		return fmt.Errorf("read vless options: %w", err)
	}
	return nil
}

// WriteResponse writes a minimal successful VLESS response.
func WriteResponse(w io.Writer) error {
	if _, err := w.Write([]byte{Version, 0}); err != nil {
		return fmt.Errorf("write vless response: %w", err)
	}
	return nil
}

// ReadResponse reads a minimal VLESS response.
func ReadResponse(r io.Reader) error {
	var fixed [2]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return fmt.Errorf("read vless response: %w", err)
	}
	if fixed[0] != Version {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, fixed[0])
	}
	if optLen := int(fixed[1]); optLen > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(optLen)); err != nil {
			return fmt.Errorf("read vless response options: %w", err)
		}
	}
	return nil
}

func encodeAddr(host string) (byte, []byte, error) {
	addr, err := netip.ParseAddr(host)
	if err == nil {
		if addr.Is4() {
			a4 := addr.As4()
			return AddrIPv4, a4[:], nil
		}
		a16 := addr.As16()
		return AddrIPv6, a16[:], nil
	}
	domainLen := len(host)
	if host == "" || domainLen > MaxDomainLen {
		return 0, nil, ErrInvalidAddress
	}
	encoded := make([]byte, domainLen+1)
	encoded[0] = byte(domainLen) // #nosec G115 - domainLen is bounded by MaxDomainLen above.
	copy(encoded[1:], host)
	return AddrDomain, encoded, nil
}

func readAddr(r io.Reader) (string, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return "", fmt.Errorf("read vless address type: %w", err)
	}
	switch typ[0] {
	case AddrIPv4:
		var raw [4]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return "", fmt.Errorf("read vless ipv4: %w", err)
		}
		return netip.AddrFrom4(raw).String(), nil
	case AddrIPv6:
		var raw [16]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return "", fmt.Errorf("read vless ipv6: %w", err)
		}
		return netip.AddrFrom16(raw).String(), nil
	case AddrDomain:
		return readDomain(r)
	default:
		return "", fmt.Errorf("%w: type=%d", ErrInvalidAddress, typ[0])
	}
}

func readDomain(r io.Reader) (string, error) {
	var size [1]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return "", fmt.Errorf("read vless domain length: %w", err)
	}
	if size[0] == 0 {
		return "", ErrInvalidAddress
	}
	raw := make([]byte, int(size[0]))
	if _, err := io.ReadFull(r, raw); err != nil {
		return "", fmt.Errorf("read vless domain: %w", err)
	}
	return string(raw), nil
}
