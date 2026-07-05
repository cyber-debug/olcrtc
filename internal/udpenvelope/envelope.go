// Package udpenvelope wraps encrypted UDP relay frames with sequencing and
// optional padding metadata before AEAD encryption.
package udpenvelope

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
)

const (
	// Version is the current datagram envelope version.
	Version byte = 1
	// MaxPaddingLen caps per-datagram padding so operators cannot accidentally
	// push lossy WebRTC datagrams far beyond practical MTU-sized payloads.
	MaxPaddingLen = 255
)

const (
	headerLen = 14
	flagPad   = 1 << 0
)

var magic = [2]byte{'O', 'E'} //nolint:gochecknoglobals // protocol marker

var (
	// ErrFrameTooShort is returned when an envelope is shorter than the fixed header.
	ErrFrameTooShort = errors.New("udpenvelope: frame too short")
	// ErrBadMagic is returned when an envelope does not carry the olcrtc UDP envelope marker.
	ErrBadMagic = errors.New("udpenvelope: bad magic")
	// ErrUnsupportedVersion is returned when an envelope carries an unknown version.
	ErrUnsupportedVersion = errors.New("udpenvelope: unsupported version")
	// ErrUnknownFlags is returned when an envelope has unsupported flag bits.
	ErrUnknownFlags = errors.New("udpenvelope: unknown flags")
	// ErrInvalidSequence is returned for the reserved zero sequence.
	ErrInvalidSequence = errors.New("udpenvelope: invalid sequence")
	// ErrPaddingTooLarge is returned when padding metadata is invalid.
	ErrPaddingTooLarge = errors.New("udpenvelope: padding too large")
	// ErrReplay is returned when replay protection rejects a repeated sequence.
	ErrReplay = errors.New("udpenvelope: replayed sequence")
)

// Envelope is the authenticated metadata that surrounds one UDP wire payload.
type Envelope struct {
	Sequence   uint64
	Payload    []byte
	PaddingLen int
}

// Encode serializes one datagram envelope. Padding bytes are intentionally zero:
// the whole envelope is encrypted by the caller, so plaintext padding contents do
// not appear on the network and do not need cryptographic randomness.
func Encode(sequence uint64, payload []byte, paddingLen int) ([]byte, error) {
	if sequence == 0 {
		return nil, ErrInvalidSequence
	}
	if paddingLen < 0 || paddingLen > MaxPaddingLen {
		return nil, ErrPaddingTooLarge
	}

	out := make([]byte, headerLen+len(payload)+paddingLen)
	copy(out[:len(magic)], magic[:])
	out[2] = Version
	if paddingLen > 0 {
		out[3] = flagPad
	}
	binary.BigEndian.PutUint64(out[4:12], sequence)
	binary.BigEndian.PutUint16(out[12:14], uint16(paddingLen))
	copy(out[headerLen:], payload)
	return out, nil
}

// Decode parses one datagram envelope. The returned Payload aliases data.
func Decode(data []byte) (Envelope, error) {
	sequence, paddingLen, err := decodeHeader(data)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		Sequence:   sequence,
		Payload:    data[headerLen : len(data)-paddingLen],
		PaddingLen: paddingLen,
	}, nil
}

func decodeHeader(data []byte) (uint64, int, error) {
	if len(data) < headerLen {
		return 0, 0, ErrFrameTooShort
	}
	if data[0] != magic[0] || data[1] != magic[1] {
		return 0, 0, ErrBadMagic
	}
	if data[2] != Version {
		return 0, 0, fmt.Errorf("%w: %d", ErrUnsupportedVersion, data[2])
	}
	flags := data[3]
	if err := validateFlags(flags); err != nil {
		return 0, 0, err
	}
	sequence := binary.BigEndian.Uint64(data[4:12])
	if sequence == 0 {
		return 0, 0, ErrInvalidSequence
	}
	paddingLen := int(binary.BigEndian.Uint16(data[12:14]))
	if err := validatePadding(flags, paddingLen, len(data)-headerLen); err != nil {
		return 0, 0, err
	}
	return sequence, paddingLen, nil
}

func validateFlags(flags byte) error {
	if flags&^flagPad != 0 {
		return fmt.Errorf("%w: 0x%x", ErrUnknownFlags, flags)
	}
	return nil
}

func validatePadding(flags byte, paddingLen, bodyLen int) error {
	if paddingLen > MaxPaddingLen || paddingLen > bodyLen {
		return ErrPaddingTooLarge
	}
	if paddingLen == 0 && flags&flagPad != 0 {
		return ErrPaddingTooLarge
	}
	if paddingLen > 0 && flags&flagPad == 0 {
		return ErrPaddingTooLarge
	}
	return nil
}

// RandomPaddingLen returns a random padding length in [0, max]. This is traffic
// shaping randomness, not secret material.
func RandomPaddingLen(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > MaxPaddingLen {
		limit = MaxPaddingLen
	}
	return rand.IntN(limit + 1) //nolint:gosec // padding-size jitter is non-cryptographic.
}

// ReplayWindow tracks a sliding 64-packet acceptance window.
type ReplayWindow struct {
	maxSequence uint64
	seen        uint64
	initialized bool
}

// Accept returns true once for each sequence in the current replay window.
func (w *ReplayWindow) Accept(sequence uint64) bool {
	if sequence == 0 {
		return false
	}
	if !w.initialized {
		w.initialized = true
		w.maxSequence = sequence
		w.seen = 1
		return true
	}
	if sequence > w.maxSequence {
		delta := sequence - w.maxSequence
		if delta >= 64 {
			w.seen = 1
		} else {
			w.seen = (w.seen << delta) | 1
		}
		w.maxSequence = sequence
		return true
	}

	offset := w.maxSequence - sequence
	if offset >= 64 {
		return false
	}
	mask := uint64(1) << offset
	if w.seen&mask != 0 {
		return false
	}
	w.seen |= mask
	return true
}
