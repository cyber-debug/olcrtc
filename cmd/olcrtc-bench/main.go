package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc"
)

const (
	modeClient        = "client"
	modeServer        = "server"
	transportStream   = "stream"
	transportDatagram = "datagram"
	directionDownload = "download"
	directionUpload   = "upload"
	datagramHeaderLen = 16
)

type benchConfig struct {
	mode          string
	transport     string
	auth          string
	room          string
	name          string
	dns           string
	bytes         int64
	chunk         int
	count         int
	direction     string
	interval      time.Duration
	packetTimeout time.Duration
	timeout       time.Duration
}

func main() {
	cfg := benchConfig{}
	flag.StringVar(&cfg.mode, "mode", modeClient, "client or server")
	flag.StringVar(&cfg.transport, "transport", transportStream, "stream or datagram")
	flag.StringVar(&cfg.auth, "auth", "jitsi", "auth provider")
	flag.StringVar(&cfg.room, "room", "", "room URL")
	flag.StringVar(&cfg.name, "name", "", "participant name")
	flag.StringVar(&cfg.dns, "dns", "1.1.1.1:53", "DNS server")
	flag.Int64Var(&cfg.bytes, "bytes", 10*1024*1024, "bytes to transfer")
	flag.IntVar(&cfg.chunk, "chunk", 12*1024, "write chunk size")
	flag.IntVar(&cfg.count, "count", 1, "request/datagram count; 0 means until timeout")
	flag.StringVar(&cfg.direction, "direction", directionDownload, "download or upload")
	flag.DurationVar(&cfg.interval, "interval", 0, "delay between client iterations")
	flag.DurationVar(&cfg.packetTimeout, "packet-timeout", 5*time.Second, "datagram reply timeout")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "benchmark timeout")
	flag.Parse()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg benchConfig) error {
	if cfg.room == "" {
		return fmt.Errorf("-room is required")
	}
	if cfg.chunk <= 0 {
		return fmt.Errorf("-chunk must be positive")
	}
	if cfg.bytes < 0 {
		return fmt.Errorf("-bytes must be non-negative")
	}
	if cfg.count < 0 {
		return fmt.Errorf("-count must be non-negative")
	}
	if cfg.packetTimeout <= 0 {
		return fmt.Errorf("-packet-timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	olcrtc.RegisterDefaults()
	sess, err := olcrtc.New(ctx, olcrtc.Config{
		Auth:      cfg.auth,
		RoomID:    cfg.room,
		Name:      cfg.name,
		DNSServer: cfg.dns,
	})
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	defer func() { _ = sess.Close() }()

	switch cfg.transport {
	case transportStream:
		return runStream(ctx, sess, cfg)
	case transportDatagram:
		return runDatagram(ctx, sess, cfg)
	default:
		return fmt.Errorf("unsupported transport %q", cfg.transport)
	}
}

func runStream(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	switch cfg.mode {
	case modeServer:
		return runStreamServer(ctx, sess, cfg)
	case modeClient:
		return runStreamClient(ctx, sess, cfg)
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
}

func runStreamServer(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	conn, err := sess.AcceptStream(ctx)
	if err != nil {
		return fmt.Errorf("accept stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	closeOnContext(ctx, conn)

	reader := bufio.NewReader(conn)
	for i := 0; shouldContinue(i, cfg.count); i++ {
		req, err := readRequest(reader)
		if err != nil {
			if cfg.count == 0 && err == io.EOF {
				return nil
			}
			return fmt.Errorf("read request: %w", err)
		}
		if err := serveStreamRequest(conn, reader, req, cfg.chunk); err != nil {
			return fmt.Errorf("serve %s iteration %d: %w", req.direction, i+1, err)
		}
	}
	return nil
}

func runStreamClient(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	closeOnContext(ctx, conn)
	reader := bufio.NewReader(conn)

	for i := 0; shouldContinue(i, cfg.count); i++ {
		if i > 0 && cfg.interval > 0 {
			if err := sleepContext(ctx, cfg.interval); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(conn, "BENCH/1 %s %d\n", cfg.direction, cfg.bytes); err != nil {
			return fmt.Errorf("write request iteration %d: %w", i+1, err)
		}
		n, elapsed, err := runStreamClientIteration(conn, reader, cfg)
		if err != nil {
			return fmt.Errorf("client %s iteration %d after %d bytes: %w", cfg.direction, i+1, n, err)
		}
		printResult(cfg.direction, i+1, n, elapsed)
	}
	return nil
}

func runStreamClientIteration(conn io.Writer, reader *bufio.Reader, cfg benchConfig) (int64, time.Duration, error) {
	start := time.Now()
	var n int64
	var err error
	switch cfg.direction {
	case directionDownload:
		n, err = io.CopyN(io.Discard, reader, cfg.bytes)
		if err == nil {
			_, err = io.WriteString(conn, "OK\n")
		}
	case directionUpload:
		n, err = writeBytes(conn, cfg.bytes, cfg.chunk)
		if err == nil {
			if err = readToken(reader, "OK"); err == nil {
				_, err = io.WriteString(conn, "DONE\n")
			}
		}
	default:
		return 0, 0, fmt.Errorf("unsupported direction %q", cfg.direction)
	}
	elapsed := time.Since(start)
	return n, elapsed, err
}

type benchRequest struct {
	direction string
	bytes     int64
}

func parseRequest(line string) (benchRequest, error) {
	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "BENCH/1" {
		return benchRequest{}, fmt.Errorf("invalid request %q", strings.TrimSpace(line))
	}
	bytes, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || bytes < 0 {
		return benchRequest{}, fmt.Errorf("invalid bytes %q", parts[2])
	}
	return benchRequest{direction: parts[1], bytes: bytes}, nil
}

func readRequest(reader *bufio.Reader) (benchRequest, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return benchRequest{}, err
	}
	return parseRequest(header)
}

func serveStreamRequest(conn io.Writer, reader *bufio.Reader, req benchRequest, chunk int) error {
	var err error
	switch req.direction {
	case directionDownload:
		if _, err = writeBytes(conn, req.bytes, chunk); err == nil {
			err = readToken(reader, "OK")
		}
	case directionUpload:
		if _, err = io.CopyN(io.Discard, reader, req.bytes); err == nil {
			if _, err = io.WriteString(conn, "OK\n"); err == nil {
				err = readToken(reader, "DONE")
			}
		}
	default:
		err = fmt.Errorf("unsupported direction %q", req.direction)
	}
	return err
}

func runDatagram(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	if !sess.Capabilities().LossyDatagrams {
		return fmt.Errorf("selected olcrtc engine does not support datagrams")
	}
	if cfg.mode == modeClient && cfg.bytes > 1200 {
		return fmt.Errorf("-bytes must be <= 1200 for datagram transport")
	}
	if cfg.mode == modeClient && cfg.bytes < datagramHeaderLen {
		cfg.bytes = datagramHeaderLen
	}
	if err := connectDatagramSession(ctx, sess); err != nil {
		return err
	}
	switch cfg.mode {
	case modeServer:
		return runDatagramServer(ctx, sess, cfg)
	case modeClient:
		return runDatagramClient(ctx, sess, cfg)
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
}

func connectDatagramSession(ctx context.Context, sess *olcrtc.Session) error {
	if err := sess.Connect(ctx); err != nil {
		return fmt.Errorf("connect datagram session: %w", err)
	}
	go sess.WatchConnection(ctx)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if sess.CanSend() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait datagram ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func runDatagramServer(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	for i := 0; shouldContinue(i, cfg.count); i++ {
		packet, err := sess.ReceivePeerDatagram(ctx)
		if err != nil {
			return fmt.Errorf("receive datagram iteration %d: %w", i+1, err)
		}
		if packet.PeerID != "" {
			err = sess.SendDatagramTo(ctx, packet.PeerID, packet.Payload)
		} else {
			err = sess.SendDatagram(ctx, packet.Payload)
		}
		if err != nil {
			return fmt.Errorf("echo datagram iteration %d: %w", i+1, err)
		}
	}
	return nil
}

func runDatagramClient(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	stats := datagramStats{}
	for i := 0; shouldContinue(i, cfg.count); i++ {
		if i > 0 && cfg.interval > 0 {
			if err := sleepContext(ctx, cfg.interval); err != nil {
				return err
			}
		}
		seq := uint32(i + 1)
		payload := datagramPayload(seq, int(cfg.bytes))
		start := time.Now()
		stats.sent++
		if err := sess.SendDatagram(ctx, payload); err != nil {
			return fmt.Errorf("send datagram seq %d: %w", seq, err)
		}
		packetCtx, cancel := context.WithTimeout(ctx, cfg.packetTimeout)
		rtt, err := waitDatagramEcho(packetCtx, sess, seq, start)
		cancel()
		if err != nil {
			stats.lost++
			fmt.Fprintf(os.Stdout, "datagram seq:%d bytes:%d lost:true error:%v\n", seq, len(payload), err)
			continue
		}
		stats.record(rtt)
		fmt.Fprintf(os.Stdout, "datagram seq:%d bytes:%d rtt_ms:%.3f\n", seq, len(payload), float64(rtt.Microseconds())/1000)
	}
	stats.print()
	return nil
}

func datagramPayload(seq uint32, size int) []byte {
	payload := make([]byte, size)
	copy(payload[:4], []byte{'O', 'L', 'D', '1'})
	binary.BigEndian.PutUint32(payload[4:8], seq)
	binary.BigEndian.PutUint64(payload[8:16], uint64(time.Now().UnixNano()))
	for i := datagramHeaderLen; i < len(payload); i++ {
		payload[i] = byte(seq + uint32(i))
	}
	return payload
}

func datagramSeq(payload []byte) (uint32, bool) {
	if len(payload) < datagramHeaderLen || string(payload[:4]) != "OLD1" {
		return 0, false
	}
	return binary.BigEndian.Uint32(payload[4:8]), true
}

func waitDatagramEcho(ctx context.Context, sess *olcrtc.Session, seq uint32, start time.Time) (time.Duration, error) {
	for {
		packet, err := sess.ReceivePeerDatagram(ctx)
		if err != nil {
			return 0, err
		}
		gotSeq, ok := datagramSeq(packet.Payload)
		if ok && gotSeq == seq {
			return time.Since(start), nil
		}
	}
}

type datagramStats struct {
	sent     int
	received int
	lost     int
	min      time.Duration
	max      time.Duration
	total    time.Duration
}

func (s *datagramStats) record(rtt time.Duration) {
	s.received++
	if s.min == 0 || rtt < s.min {
		s.min = rtt
	}
	if rtt > s.max {
		s.max = rtt
	}
	s.total += rtt
}

func (s datagramStats) print() {
	avg := time.Duration(0)
	if s.received > 0 {
		avg = s.total / time.Duration(s.received)
	}
	lossPct := 0.0
	if s.sent > 0 {
		lossPct = float64(s.lost) * 100 / float64(s.sent)
	}
	fmt.Fprintf(os.Stdout,
		"datagram_summary sent:%d received:%d lost:%d loss_pct:%.2f min_ms:%.3f avg_ms:%.3f max_ms:%.3f\n",
		s.sent,
		s.received,
		s.lost,
		lossPct,
		float64(s.min.Microseconds())/1000,
		float64(avg.Microseconds())/1000,
		float64(s.max.Microseconds())/1000,
	)
}

func readToken(reader *bufio.Reader, want string) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read %s: %w", strings.ToLower(want), err)
	}
	if strings.TrimSpace(line) != want {
		return fmt.Errorf("invalid %s %q", strings.ToLower(want), strings.TrimSpace(line))
	}
	return nil
}

func writeBytes(w io.Writer, total int64, chunk int) (int64, error) {
	buf := make([]byte, chunk)
	var written int64
	for written < total {
		n := int64(len(buf))
		if remaining := total - written; remaining < n {
			n = remaining
		}
		out, err := w.Write(buf[:n])
		written += int64(out)
		if err != nil {
			return written, err
		}
		if out == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}

func closeOnContext(ctx context.Context, closer io.Closer) {
	go func() {
		<-ctx.Done()
		_ = closer.Close()
	}()
}

func shouldContinue(iteration int, count int) bool {
	return count == 0 || iteration < count
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func printResult(direction string, iteration int, bytes int64, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	bps := float64(bytes) / seconds
	mbps := bps * 8 / 1_000_000
	fmt.Fprintf(os.Stdout, "direction:%s iteration:%d bytes:%d seconds:%.3f Bps:%.0f Mbps:%.2f\n",
		direction, iteration, bytes, seconds, bps, mbps)
}
