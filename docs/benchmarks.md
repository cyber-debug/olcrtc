# olcRTC transport benchmarks

`olcrtc-bench` measures the raw olcRTC stream layer without Xray or application
traffic on top. It is intended to separate carrier/provider limits from proxy
protocol overhead.

Build:

```sh
go build -o /tmp/olcrtc-bench ./cmd/olcrtc-bench
```

Run a server participant:

```sh
/tmp/olcrtc-bench \
  -mode server \
  -room https://meet.example.org/room-id \
  -name srv \
  -timeout 60s
```

Run a client participant:

```sh
/tmp/olcrtc-bench \
  -mode client \
  -room https://meet.example.org/room-id \
  -name cnc \
  -direction download \
  -bytes 1048576 \
  -timeout 60s
```

Use `-direction upload` to reverse the data path. Both directions require an
application-level acknowledgement, so a successful result means the peer read
the requested byte count. It is not just a local `Write` into a buffer.

For long-running stream checks, keep the same server process open and repeat
requests over one carrier:

```sh
/tmp/olcrtc-bench \
  -mode server \
  -room https://meet.example.org/room-id \
  -name srv \
  -count 20 \
  -timeout 10m

/tmp/olcrtc-bench \
  -mode client \
  -room https://meet.example.org/room-id \
  -name cnc \
  -direction download \
  -bytes 1048576 \
  -count 20 \
  -interval 15s \
  -timeout 10m
```

Use `-count 0` on the server to accept requests until `-timeout` expires.

For UDP/datagram checks, use the datagram transport mode. The client sends one
lossy datagram at a time and waits for an echoed reply, then prints per-packet
RTT and a loss summary.

This requires an engine with datagram capability. LiveKit supports this path.
The current Jitsi datachannel engine is byte-stream only, so
`-transport datagram` will fail fast there instead of pretending UDP has been
tested.

```sh
/tmp/olcrtc-bench \
  -mode server \
  -transport datagram \
  -room https://meet.example.org/room-id \
  -name srv \
  -count 100 \
  -timeout 5m

/tmp/olcrtc-bench \
  -mode client \
  -transport datagram \
  -room https://meet.example.org/room-id \
  -name cnc \
  -bytes 256 \
  -count 100 \
  -interval 100ms \
  -packet-timeout 3s \
  -timeout 5m
```

Example result from a public Jitsi room between the local host and the Latvia
test server:

```text
direction:download iteration:1 bytes:1048576 seconds:1.684 Bps:622541 Mbps:4.98
direction:upload iteration:1 bytes:1048576 seconds:1.629 Bps:643683 Mbps:5.15
datagram_summary sent:100 received:100 lost:0 loss_pct:0.00 min_ms:42.100 avg_ms:61.250 max_ms:110.800
```

Public Jitsi rooms are useful for smoke tests, but they should not be treated as
a production performance target. A public SFU may rotate bridges, reject relay
candidates, close DTLS quickly after short-lived tests, or throttle bridge data.
For production testing, benchmark every intended provider/profile separately and
prefer a controlled SFU when you need repeatable capacity numbers.

When comparing raw olcRTC with Xray-over-olcRTC, use the same provider, room
host, byte count, direction, and time window. A fair comparison is:

1. raw download/upload with `olcrtc-bench`
2. Xray download/upload through the SOCKS inbound
3. raw datagram RTT/loss with `olcrtc-bench -transport datagram`
4. Xray UDP/XUDP RTT/loss through the SOCKS UDP path

If raw and Xray numbers are close, the carrier/provider is the bottleneck. If
Xray is materially slower than raw on the same room, investigate Xray transport
framing, packet deadlines, and stream close behavior.
