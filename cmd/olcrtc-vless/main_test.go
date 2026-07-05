package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
mode: client
olcrtc:
  auth: jitsi
  roomId: https://meet.example/room
vless:
  userId: 11111111-1111-1111-1111-111111111111
socks:
  listen: 127.0.0.1:1080
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.Mode != modeClient || cfg.OlcRTC.RoomID != "https://meet.example/room" ||
		cfg.VLESS.UserID != "11111111-1111-1111-1111-111111111111" ||
		cfg.SOCKS.Listen != "127.0.0.1:1080" {
		t.Fatalf("loadConfig() = %+v", cfg)
	}
}

func TestLoadConfigRequiresClientSOCKSListen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`
mode: client
vless:
  userId: 11111111-1111-1111-1111-111111111111
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	_, err := loadConfig(path)
	if !errors.Is(err, errSOCKSListenRequired) {
		t.Fatalf("loadConfig() error = %v, want %v", err, errSOCKSListenRequired)
	}
}

func TestReadSOCKSRequestDomain(t *testing.T) {
	raw := make([]byte, 0, 18)
	raw = append(raw,
		5, 1, 0, 3,
		byte(len("example.com")),
	)
	raw = append(raw, []byte("example.com")...)
	raw = append(raw, 0x01, 0xbb)
	got, err := readSOCKSRequest(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("readSOCKSRequest() error = %v", err)
	}
	if got.host != "example.com" || got.port != 443 {
		t.Fatalf("readSOCKSRequest() = %+v, want example.com:443", got)
	}
}

func TestReadSOCKSMethodsRejectsVersion(t *testing.T) {
	err := readSOCKSMethods(bytes.NewReader([]byte{4, 0}))
	if !errors.Is(err, errUnsupportedSOCKSVersion) {
		t.Fatalf("readSOCKSMethods() error = %v, want %v", err, errUnsupportedSOCKSVersion)
	}
}
