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

Example result from a public Jitsi room between the local host and the Latvia
test server:

```text
direction:download bytes:1048576 seconds:1.684 Bps:622541 Mbps:4.98
direction:upload bytes:1048576 seconds:1.629 Bps:643683 Mbps:5.15
```

Public Jitsi rooms are useful for smoke tests, but they should not be treated as
a production performance target. A public SFU may rotate bridges, reject relay
candidates, close DTLS quickly after short-lived tests, or throttle bridge data.
For production testing, benchmark every intended provider/profile separately and
prefer a controlled SFU when you need repeatable capacity numbers.
