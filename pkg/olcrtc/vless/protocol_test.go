package vless

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	testHostName = "example.com"
	testEchoHost = "echo.local"
)

func TestRequestRoundTrip(t *testing.T) {
	testUserID := vlessTestUserID()
	var b bytes.Buffer
	want := Request{UserID: testUserID, Command: CommandTCP, Host: testHostName, Port: 443}
	if err := WriteRequest(&b, want); err != nil {
		t.Fatalf("WriteRequest() error = %v", err)
	}
	got, err := ReadRequest(&b, map[uuid.UUID]struct{}{testUserID: {}})
	if err != nil {
		t.Fatalf("ReadRequest() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadRequest() = %+v, want %+v", got, want)
	}
}

func TestReadRequestRejectsUnauthorizedUser(t *testing.T) {
	var b bytes.Buffer
	if err := WriteRequest(&b, Request{
		UserID:  vlessTestUserID(),
		Command: CommandTCP,
		Host:    testHostName,
		Port:    443,
	}); err != nil {
		t.Fatalf("WriteRequest() error = %v", err)
	}
	_, err := ReadRequest(&b, map[uuid.UUID]struct{}{uuid.New(): {}})
	if !errors.Is(err, ErrUnauthorizedUser) {
		t.Fatalf("ReadRequest(unauthorized) error = %v, want %v", err, ErrUnauthorizedUser)
	}
}

func TestAdapterTCPEcho(t *testing.T) {
	testUserID := vlessTestUserID()
	clientTransport, serverTransport := net.Pipe()
	defer func() { _ = clientTransport.Close() }()
	defer func() { _ = serverTransport.Close() }()
	localClient, localApp := net.Pipe()
	defer func() { _ = localClient.Close() }()
	defer func() { _ = localApp.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- ServeTCP(ctx, serverTransport, map[uuid.UUID]struct{}{testUserID: {}}, echoDial)
	}()
	clientErr := make(chan error, 1)
	go func() {
		clientErr <- ClientTCP(ctx, localClient, clientTransport, Request{
			UserID: testUserID,
			Host:   testEchoHost,
			Port:   443,
		})
	}()

	if _, err := localApp.Write([]byte("ping")); err != nil {
		t.Fatalf("write local app: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(localApp, buf); err != nil {
		t.Fatalf("read local app: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo = %q, want ping", buf)
	}
	_ = localApp.Close()

	if err := <-clientErr; err != nil {
		t.Fatalf("ClientTCP() error = %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("ServeTCP() error = %v", err)
	}
}

func vlessTestUserID() uuid.UUID {
	return uuid.MustParse("11111111-1111-1111-1111-111111111111")
}

func echoDial(_ context.Context, _, _ string) (net.Conn, error) {
	left, right := net.Pipe()
	go func() {
		defer func() { _ = right.Close() }()
		_, _ = io.Copy(right, right)
	}()
	return left, nil
}
