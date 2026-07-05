package vless

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/transportapi"
)

// DialContext dials the target requested by a VLESS stream.
type DialContext func(ctx context.Context, network, address string) (net.Conn, error)

// ServeTCP accepts one VLESS TCP request on transport, dials its target and
// proxies bytes until either side closes.
func ServeTCP(ctx context.Context, transport net.Conn, allowed map[uuid.UUID]struct{}, dial DialContext) error {
	defer func() { _ = transport.Close() }()
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	req, err := ReadRequest(transport, allowed)
	if err != nil {
		return err
	}
	if req.Command == CommandUDP {
		return ErrUDPUnsupported
	}
	target, err := dial(ctx, "tcp", req.Target())
	if err != nil {
		return fmt.Errorf("dial vless target %s: %w", req.Target(), err)
	}
	defer func() { _ = target.Close() }()
	if err := WriteResponse(transport); err != nil {
		return err
	}
	_, err = Proxy(ctx, transport, target, nil)
	return err
}

// ClientTCP writes a VLESS TCP request and proxies local bytes over transport.
func ClientTCP(ctx context.Context, local, transport net.Conn, req Request) error {
	defer func() { _ = transport.Close() }()
	if req.Command == 0 {
		req.Command = CommandTCP
	}
	if err := WriteRequest(transport, req); err != nil {
		return err
	}
	if err := ReadResponse(transport); err != nil {
		return err
	}
	_, err := Proxy(ctx, local, transport, nil)
	return err
}

// ProxyStats summarizes one bidirectional proxy run.
type ProxyStats struct {
	BytesAB int64
	BytesBA int64
}

type proxyResult struct {
	ab  bool
	n   int64
	err error
}

// Proxy copies bytes between a and b until both directions finish or ctx is cancelled.
func Proxy(ctx context.Context, a, b net.Conn, counters *transportapi.Counters) (ProxyStats, error) {
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan proxyResult, 2)
	go copyConn(ctx, a, b, true, counters, results)
	go copyConn(ctx, b, a, false, counters, results)

	var stats ProxyStats
	var errs []error
	for i := range 2 {
		res := <-results
		if res.ab {
			stats.BytesAB = res.n
		} else {
			stats.BytesBA = res.n
		}
		if res.err != nil && !isExpectedProxyClose(res.err) {
			errs = append(errs, res.err)
		}
		if i == 0 {
			cancel()
			_ = a.SetDeadline(time.Now())
			_ = b.SetDeadline(time.Now())
		}
	}
	if len(errs) == 0 && parentCtx.Err() != nil {
		return stats, fmt.Errorf("proxy context: %w", parentCtx.Err())
	}
	return stats, errors.Join(errs...)
}

func isExpectedProxyClose(err error) bool {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func copyConn(
	ctx context.Context,
	dst net.Conn,
	src net.Conn,
	ab bool,
	counters *transportapi.Counters,
	results chan<- proxyResult,
) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = src.SetReadDeadline(time.Now())
			_ = dst.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	n, err := io.Copy(dst, src)
	close(done)
	if counters != nil {
		if ab {
			counters.BytesOut(n)
		} else {
			counters.BytesIn(n)
		}
	}
	results <- proxyResult{ab: ab, n: n, err: err}
}
