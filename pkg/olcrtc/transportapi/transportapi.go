// Package transportapi defines the stable boundary for using olcrtc as a
// pluggable transport under higher-level protocols such as VLESS.
package transportapi

import (
	"context"
	"net"
	"sync/atomic"
	"time"
)

const (
	// ProtocolVersion is the public transport API contract version.
	ProtocolVersion = 1
	// MaxDatagramPayload is the recommended upper bound for one lossy datagram.
	MaxDatagramPayload = 1200
)

// Capabilities describes what a transport session can carry.
type Capabilities struct {
	Version          int
	ReliableStreams  bool
	OrderedStreams   bool
	LossyDatagrams   bool
	MaxDatagramBytes int
	Reconnects       bool
}

// DefaultCapabilities returns the baseline olcrtc transport contract.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		Version:          ProtocolVersion,
		ReliableStreams:  true,
		OrderedStreams:   true,
		LossyDatagrams:   true,
		MaxDatagramBytes: MaxDatagramPayload,
		Reconnects:       true,
	}
}

// Dialer opens reliable ordered byte streams over a transport session.
type Dialer interface {
	OpenStream(ctx context.Context) (net.Conn, error)
	Capabilities() Capabilities
}

// Listener accepts reliable ordered byte streams over a transport session.
type Listener interface {
	AcceptStream(ctx context.Context) (net.Conn, error)
	Capabilities() Capabilities
}

// DatagramSender sends unordered lossy datagrams.
type DatagramSender interface {
	SendDatagram(ctx context.Context, payload []byte) error
}

// DatagramReceiver receives unordered lossy datagrams.
type DatagramReceiver interface {
	ReceiveDatagram(ctx context.Context) ([]byte, error)
}

// Metrics is a point-in-time transport snapshot.
type Metrics struct {
	OpenedStreams   uint64
	ClosedStreams   uint64
	BytesIn         uint64
	BytesOut        uint64
	DatagramsIn     uint64
	DatagramsOut    uint64
	DatagramDrops   uint64
	Reconnects      uint64
	LastActivityUTC time.Time
}

// Counters stores process-local transport counters.
type Counters struct {
	openedStreams atomic.Uint64
	closedStreams atomic.Uint64
	bytesIn       atomic.Uint64
	bytesOut      atomic.Uint64
	datagramsIn   atomic.Uint64
	datagramsOut  atomic.Uint64
	datagramDrops atomic.Uint64
	reconnects    atomic.Uint64
	lastActivity  atomic.Int64
}

// StreamOpened records one opened reliable stream.
func (c *Counters) StreamOpened() { c.touch(); c.openedStreams.Add(1) }

// StreamClosed records one closed reliable stream.
func (c *Counters) StreamClosed() { c.touch(); c.closedStreams.Add(1) }

// BytesIn records bytes read from the transport.
func (c *Counters) BytesIn(n int64) {
	if n > 0 {
		c.touch()
		c.bytesIn.Add(uint64(n))
	}
}

// BytesOut records bytes written to the transport.
func (c *Counters) BytesOut(n int64) {
	if n > 0 {
		c.touch()
		c.bytesOut.Add(uint64(n))
	}
}

// DatagramIn records one inbound datagram.
func (c *Counters) DatagramIn() { c.touch(); c.datagramsIn.Add(1) }

// DatagramOut records one outbound datagram.
func (c *Counters) DatagramOut() { c.touch(); c.datagramsOut.Add(1) }

// DatagramDrop records one dropped datagram.
func (c *Counters) DatagramDrop() { c.touch(); c.datagramDrops.Add(1) }

// Reconnect records one transport reconnect.
func (c *Counters) Reconnect() { c.touch(); c.reconnects.Add(1) }

// Snapshot returns an immutable counter view.
func (c *Counters) Snapshot() Metrics {
	if c == nil {
		return Metrics{}
	}
	return Metrics{
		OpenedStreams:   c.openedStreams.Load(),
		ClosedStreams:   c.closedStreams.Load(),
		BytesIn:         c.bytesIn.Load(),
		BytesOut:        c.bytesOut.Load(),
		DatagramsIn:     c.datagramsIn.Load(),
		DatagramsOut:    c.datagramsOut.Load(),
		DatagramDrops:   c.datagramDrops.Load(),
		Reconnects:      c.reconnects.Load(),
		LastActivityUTC: time.Unix(0, c.lastActivity.Load()).UTC(),
	}
}

func (c *Counters) touch() {
	c.lastActivity.Store(time.Now().UTC().UnixNano())
}
