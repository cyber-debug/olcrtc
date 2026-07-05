// Package main provides a small VLESS-over-olcrtc bridge CLI.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/vless"
	"github.com/xtaci/smux"
	"gopkg.in/yaml.v3"
)

const (
	modeClient = "client"
	modeServer = "server"
)

var (
	errUsage                   = errors.New("usage: olcrtc-vless <config.yaml>")
	errUnsupportedMode         = errors.New("unsupported mode")
	errVLESSUserRequired       = errors.New("vless.user_id required")
	errSOCKSListenRequired     = errors.New("socks.listen required in client mode")
	errUnsupportedSOCKSVersion = errors.New("unsupported socks version")
	errUnsupportedSOCKSRequest = errors.New("unsupported socks request")
	errUnsupportedSOCKSAddress = errors.New("unsupported socks address type")
)

type configFile struct {
	Mode   string       `yaml:"mode"`
	OlcRTC olcrtcConfig `yaml:"olcrtc"`
	VLESS  vlessConfig  `yaml:"vless"`
	SOCKS  socksConfig  `yaml:"socks"`
}

type olcrtcConfig struct {
	Auth           string `yaml:"auth"`
	RoomID         string `yaml:"roomId"`
	Engine         string `yaml:"engine"`
	URL            string `yaml:"url"`
	Token          string `yaml:"token"`
	Name           string `yaml:"name"`
	DNSServer      string `yaml:"dns"`
	ProxyAddr      string `yaml:"proxyAddr"`
	ProxyPort      int    `yaml:"proxyPort"`
	DatagramBuffer int    `yaml:"datagramBuffer"`
}

type vlessConfig struct {
	UserID             string `yaml:"userId"`
	UDPIdleTimeout     string `yaml:"udpIdleTimeout"`
	UDPReadTimeout     string `yaml:"udpReadTimeout"`
	MaxUDPAssociations int    `yaml:"maxUdpAssociations"`
}

type socksConfig struct {
	Listen string `yaml:"listen"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 || args[0] == "-h" || args[0] == "--help" {
		return errUsage
	}
	cfg, err := loadConfig(args[0])
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch cfg.Mode {
	case modeClient:
		return runClient(ctx, cfg)
	case modeServer:
		return runServer(ctx, cfg)
	default:
		return fmt.Errorf("%w: %q", errUnsupportedMode, cfg.Mode)
	}
}

func loadConfig(path string) (configFile, error) {
	// #nosec G304 G703 - config path is explicit CLI input.
	data, err := os.ReadFile(path)
	if err != nil {
		return configFile{}, fmt.Errorf("read config: %w", err)
	}
	var cfg configFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return configFile{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Mode == "" {
		cfg.Mode = modeClient
	}
	if cfg.VLESS.UserID == "" {
		return configFile{}, errVLESSUserRequired
	}
	if cfg.Mode == modeClient && cfg.SOCKS.Listen == "" {
		return configFile{}, errSOCKSListenRequired
	}
	return cfg, nil
}

func runClient(ctx context.Context, cfg configFile) error {
	userID, err := uuid.Parse(cfg.VLESS.UserID)
	if err != nil {
		return fmt.Errorf("parse vless.user_id: %w", err)
	}
	sess, err := newSession(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()
	conn, err := sess.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("open olcrtc stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	mux, err := smux.Client(conn, smux.DefaultConfig())
	if err != nil {
		return fmt.Errorf("open smux client: %w", err)
	}
	defer func() { _ = mux.Close() }()
	ln, err := listenTCP(ctx, cfg.SOCKS.Listen)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	log.Printf("olcrtc-vless socks listening on %s", cfg.SOCKS.Listen)
	go closeListenerOnCancel(ctx, ln)
	for {
		local, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept socks: %w", err)
		}
		go handleSOCKSConn(ctx, local, mux, userID)
	}
}

func runServer(ctx context.Context, cfg configFile) error {
	userID, err := uuid.Parse(cfg.VLESS.UserID)
	if err != nil {
		return fmt.Errorf("parse vless.user_id: %w", err)
	}
	sess, err := newSession(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()
	allowed := map[uuid.UUID]struct{}{userID: {}}
	go serveUDPRelay(ctx, sess, allowed, cfg.VLESS)
	conn, err := sess.AcceptStream(ctx)
	if err != nil {
		return fmt.Errorf("accept olcrtc stream: %w", err)
	}
	defer func() { _ = conn.Close() }()
	mux, err := smux.Server(conn, smux.DefaultConfig())
	if err != nil {
		return fmt.Errorf("open smux server: %w", err)
	}
	defer func() { _ = mux.Close() }()
	log.Print("olcrtc-vless server ready")
	for {
		stream, err := mux.AcceptStream()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept smux stream: %w", err)
		}
		go func() {
			if err := vless.ServeTCP(ctx, stream, allowed, nil); err != nil {
				log.Printf("vless tcp stream ended: %v", err)
			}
		}()
	}
}

func serveUDPRelay(
	ctx context.Context,
	sess *olcrtc.Session,
	allowed map[uuid.UUID]struct{},
	cfg vlessConfig,
) {
	err := vless.ServeUDP(ctx, sess, sess, allowed, nil, udpRelayConfig(cfg))
	if err != nil && ctx.Err() == nil {
		log.Printf("vless udp relay stopped: %v", err)
	}
}

func newSession(ctx context.Context, cfg configFile) (*olcrtc.Session, error) {
	olcrtc.RegisterDefaults()
	sess, err := olcrtc.New(ctx, olcrtc.Config{
		Auth:           cfg.OlcRTC.Auth,
		RoomID:         cfg.OlcRTC.RoomID,
		Engine:         cfg.OlcRTC.Engine,
		URL:            cfg.OlcRTC.URL,
		Token:          cfg.OlcRTC.Token,
		Name:           cfg.OlcRTC.Name,
		DNSServer:      cfg.OlcRTC.DNSServer,
		ProxyAddr:      cfg.OlcRTC.ProxyAddr,
		ProxyPort:      cfg.OlcRTC.ProxyPort,
		DatagramBuffer: cfg.OlcRTC.DatagramBuffer,
	})
	if err != nil {
		return nil, fmt.Errorf("new olcrtc session: %w", err)
	}
	return sess, nil
}

func udpRelayConfig(cfg vlessConfig) vless.UDPRelayConfig {
	return vless.UDPRelayConfig{
		IdleTimeout:       parseDurationOrZero(cfg.UDPIdleTimeout),
		TargetReadTimeout: parseDurationOrZero(cfg.UDPReadTimeout),
		MaxAssociations:   cfg.MaxUDPAssociations,
	}
}

func parseDurationOrZero(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return d
}

func listenTCP(ctx context.Context, addr string) (net.Listener, error) {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return ln, nil
}

func closeListenerOnCancel(ctx context.Context, ln net.Listener) {
	<-ctx.Done()
	_ = ln.Close()
}

func handleSOCKSConn(ctx context.Context, local net.Conn, mux *smux.Session, userID uuid.UUID) {
	defer func() { _ = local.Close() }()
	target, err := acceptSOCKSConnect(local)
	if err != nil {
		return
	}
	stream, err := mux.OpenStream()
	if err != nil {
		_, _ = local.Write(socksReply(5, nil))
		return
	}
	defer func() { _ = stream.Close() }()
	if _, err := local.Write(socksReply(0, local.LocalAddr())); err != nil {
		return
	}
	req := vless.Request{UserID: userID, Command: vless.CommandTCP, Host: target.host, Port: target.port}
	if err := vless.ClientTCP(ctx, local, stream, req); err != nil && ctx.Err() == nil {
		log.Printf("vless tcp client ended: %v", err)
	}
}

type socksTarget struct {
	host string
	port uint16
}

func acceptSOCKSConnect(conn net.Conn) (socksTarget, error) {
	if err := readSOCKSMethods(conn); err != nil {
		return socksTarget{}, err
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return socksTarget{}, fmt.Errorf("write socks method: %w", err)
	}
	target, err := readSOCKSRequest(conn)
	if err != nil {
		_, _ = conn.Write(socksReply(7, nil))
		return socksTarget{}, err
	}
	return target, nil
}

func readSOCKSMethods(r io.Reader) error {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fmt.Errorf("read socks methods: %w", err)
	}
	if hdr[0] != 5 {
		return fmt.Errorf("%w: %d", errUnsupportedSOCKSVersion, hdr[0])
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(r, methods); err != nil {
		return fmt.Errorf("read socks method list: %w", err)
	}
	return nil
}

func readSOCKSRequest(r io.Reader) (socksTarget, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return socksTarget{}, fmt.Errorf("read socks request: %w", err)
	}
	if hdr[0] != 5 || hdr[1] != 1 || hdr[2] != 0 {
		return socksTarget{}, fmt.Errorf("%w: version=%d command=%d", errUnsupportedSOCKSRequest, hdr[0], hdr[1])
	}
	host, err := readSOCKSAddr(r, hdr[3])
	if err != nil {
		return socksTarget{}, err
	}
	var port [2]byte
	if _, err := io.ReadFull(r, port[:]); err != nil {
		return socksTarget{}, fmt.Errorf("read socks port: %w", err)
	}
	return socksTarget{host: host, port: uint16(port[0])<<8 | uint16(port[1])}, nil
}

func readSOCKSAddr(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case 1:
		var raw [4]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return "", fmt.Errorf("read socks ipv4: %w", err)
		}
		return net.IP(raw[:]).String(), nil
	case 3:
		var size [1]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			return "", fmt.Errorf("read socks domain length: %w", err)
		}
		raw := make([]byte, int(size[0]))
		if _, err := io.ReadFull(r, raw); err != nil {
			return "", fmt.Errorf("read socks domain: %w", err)
		}
		return string(raw), nil
	case 4:
		var raw [16]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return "", fmt.Errorf("read socks ipv6: %w", err)
		}
		return net.IP(raw[:]).String(), nil
	default:
		return "", fmt.Errorf("%w: %d", errUnsupportedSOCKSAddress, atyp)
	}
}

func socksReply(code byte, addr net.Addr) []byte {
	ip := net.IPv4(0, 0, 0, 0)
	port := 0
	if tcp, ok := addr.(*net.TCPAddr); ok {
		if tcp.IP.To4() != nil {
			ip = tcp.IP.To4()
		}
		port = tcp.Port
	}
	out := []byte{5, code, 0, 1, ip[0], ip[1], ip[2], ip[3], 0, 0}
	out[8] = byte(port >> 8)
	out[9] = byte(port)
	return out
}
