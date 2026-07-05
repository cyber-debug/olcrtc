package udpenvelope

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	payload := []byte("udp-wire-frame")
	wire, err := Encode(42, payload, 12)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Sequence != 42 {
		t.Fatalf("Sequence = %d, want 42", got.Sequence)
	}
	if got.PaddingLen != 12 {
		t.Fatalf("PaddingLen = %d, want 12", got.PaddingLen)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("Payload = %q, want %q", got.Payload, payload)
	}
}

func TestDecodePayloadAliasesWireBuffer(t *testing.T) {
	wire, err := Encode(1, []byte("payload"), 4)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	wire[headerLen+len("payloa")] = 'X'

	if string(got.Payload) != "payloaX" {
		t.Fatalf("decoded payload does not alias envelope buffer: %q", got.Payload)
	}
}

func TestRejectsMalformedEnvelopes(t *testing.T) {
	valid, err := Encode(7, []byte("x"), 2)
	if err != nil {
		t.Fatalf("Encode(valid) error = %v", err)
	}

	tests := []struct {
		name string
		mut  func([]byte) []byte
		want error
	}{
		{
			name: "short",
			mut:  func([]byte) []byte { return []byte{1, 2, 3} },
			want: ErrFrameTooShort,
		},
		{
			name: "bad magic",
			mut: func(b []byte) []byte {
				b[0] = 'X'
				return b
			},
			want: ErrBadMagic,
		},
		{
			name: "bad version",
			mut: func(b []byte) []byte {
				b[2] = 99
				return b
			},
			want: ErrUnsupportedVersion,
		},
		{
			name: "unknown flags",
			mut: func(b []byte) []byte {
				b[3] = 0x80
				return b
			},
			want: ErrUnknownFlags,
		},
		{
			name: "zero sequence",
			mut: func(b []byte) []byte {
				for i := 4; i < 12; i++ {
					b[i] = 0
				}
				return b
			},
			want: ErrInvalidSequence,
		},
		{
			name: "padding beyond frame",
			mut: func(b []byte) []byte {
				b[12] = 1
				b[13] = 0
				return b
			},
			want: ErrPaddingTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := append([]byte(nil), valid...)
			_, err := Decode(tt.mut(buf))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Decode() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestReplayWindow(t *testing.T) {
	var w ReplayWindow

	for _, seq := range []uint64{10, 12, 11, 75} {
		if !w.Accept(seq) {
			t.Fatalf("Accept(%d) = false, want true", seq)
		}
	}
	for _, seq := range []uint64{10, 11, 12, 75} {
		if w.Accept(seq) {
			t.Fatalf("Accept(%d) = true, want replay rejection", seq)
		}
	}
	if !w.Accept(76) {
		t.Fatal("Accept(76) = false, want true")
	}
}

func TestRandomPaddingLenBounds(t *testing.T) {
	for range 100 {
		got := RandomPaddingLen(8)
		if got < 0 || got > 8 {
			t.Fatalf("RandomPaddingLen(8) = %d, out of bounds", got)
		}
	}
	if got := RandomPaddingLen(-1); got != 0 {
		t.Fatalf("RandomPaddingLen(-1) = %d, want 0", got)
	}
	if got := RandomPaddingLen(MaxPaddingLen + 1); got < 0 || got > MaxPaddingLen {
		t.Fatalf("RandomPaddingLen(MaxPaddingLen+1) = %d, out of bounds", got)
	}
}
