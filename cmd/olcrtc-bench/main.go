package main

import (
	"bufio"
	"context"
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
	directionDownload = "download"
	directionUpload   = "upload"
)

type benchConfig struct {
	mode      string
	auth      string
	room      string
	name      string
	dns       string
	bytes     int64
	chunk     int
	direction string
	timeout   time.Duration
}

func main() {
	cfg := benchConfig{}
	flag.StringVar(&cfg.mode, "mode", modeClient, "client or server")
	flag.StringVar(&cfg.auth, "auth", "jitsi", "auth provider")
	flag.StringVar(&cfg.room, "room", "", "room URL")
	flag.StringVar(&cfg.name, "name", "", "participant name")
	flag.StringVar(&cfg.dns, "dns", "1.1.1.1:53", "DNS server")
	flag.Int64Var(&cfg.bytes, "bytes", 10*1024*1024, "bytes to transfer")
	flag.IntVar(&cfg.chunk, "chunk", 12*1024, "write chunk size")
	flag.StringVar(&cfg.direction, "direction", directionDownload, "download or upload")
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

	switch cfg.mode {
	case modeServer:
		return runServer(ctx, sess, cfg)
	case modeClient:
		return runClient(ctx, sess, cfg)
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
}

func runServer(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	conn, err := sess.AcceptStream(ctx)
	if err != nil {
		return fmt.Errorf("accept stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	closeOnContext(ctx, conn)

	reader := bufio.NewReader(conn)
	header, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	req, err := parseRequest(header)
	if err != nil {
		return err
	}
	switch req.direction {
	case directionDownload:
		if _, err = writeBytes(conn, req.bytes, cfg.chunk); err == nil {
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
	if err != nil {
		return fmt.Errorf("serve %s: %w", req.direction, err)
	}
	return nil
}

func runClient(ctx context.Context, sess *olcrtc.Session, cfg benchConfig) error {
	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	closeOnContext(ctx, conn)

	if _, err := fmt.Fprintf(conn, "BENCH/1 %s %d\n", cfg.direction, cfg.bytes); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	start := time.Now()
	var n int64
	switch cfg.direction {
	case directionDownload:
		n, err = io.CopyN(io.Discard, conn, cfg.bytes)
		if err == nil {
			_, err = io.WriteString(conn, "OK\n")
		}
	case directionUpload:
		n, err = writeBytes(conn, cfg.bytes, cfg.chunk)
		if err == nil {
			reader := bufio.NewReader(conn)
			if err = readToken(reader, "OK"); err == nil {
				_, err = io.WriteString(conn, "DONE\n")
			}
		}
	default:
		return fmt.Errorf("unsupported direction %q", cfg.direction)
	}
	elapsed := time.Since(start)
	if err != nil {
		return fmt.Errorf("client %s after %d bytes: %w", cfg.direction, n, err)
	}
	printResult(cfg.direction, n, elapsed)
	return nil
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

func printResult(direction string, bytes int64, elapsed time.Duration) {
	seconds := elapsed.Seconds()
	bps := float64(bytes) / seconds
	mbps := bps * 8 / 1_000_000
	fmt.Fprintf(os.Stdout, "direction:%s bytes:%d seconds:%.3f Bps:%.0f Mbps:%.2f\n",
		direction, bytes, seconds, bps, mbps)
}
