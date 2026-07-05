package transportapi

import "testing"

func TestDefaultCapabilities(t *testing.T) {
	got := DefaultCapabilities()
	if got.Version != ProtocolVersion || !got.ReliableStreams || !got.OrderedStreams {
		t.Fatalf("DefaultCapabilities() = %+v", got)
	}
	if !got.LossyDatagrams || got.MaxDatagramBytes != MaxDatagramPayload {
		t.Fatalf("DefaultCapabilities() datagram contract = %+v", got)
	}
}

func TestCountersSnapshot(t *testing.T) {
	var c Counters
	c.StreamOpened()
	c.StreamClosed()
	c.BytesIn(10)
	c.BytesOut(20)
	c.DatagramIn()
	c.DatagramOut()
	c.DatagramDrop()
	c.Reconnect()
	got := c.Snapshot()
	if got.OpenedStreams != 1 || got.ClosedStreams != 1 || got.BytesIn != 10 || got.BytesOut != 20 ||
		got.DatagramsIn != 1 || got.DatagramsOut != 1 || got.DatagramDrops != 1 || got.Reconnects != 1 {
		t.Fatalf("Snapshot() = %+v", got)
	}
	if got.LastActivityUTC.IsZero() {
		t.Fatal("LastActivityUTC is zero")
	}
}
