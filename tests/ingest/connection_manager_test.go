package ingest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chimaihueze/market-stream/internal/ingest"
)

type fakeConn struct {
	messages [][]byte
	err      error
	closed   bool
	onEmpty  func()
}

func (c *fakeConn) ReadMessage() (int, []byte, error) {
	if len(c.messages) == 0 {
		if c.onEmpty != nil {
			c.onEmpty()
		}
		return 0, nil, c.err
	}

	msg := c.messages[0]
	c.messages = c.messages[1:]
	return 1, msg, nil
}

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

func TestConnectionManagerReconnectsAfterReadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := &fakeConn{err: errors.New("dropped")}
	second := &fakeConn{
		messages: [][]byte{[]byte(`{"stream":"btcusdt@aggTrade"}`)},
		err:      errors.New("done"),
		onEmpty:  cancel,
	}
	connections := []*fakeConn{first, second}
	connects := 0
	dispatched := make([]string, 0, 1)

	manager := ingest.NewConnectionManager(
		func(context.Context) (ingest.MessageConnection, error) {
			conn := connections[connects]
			connects++
			return conn, nil
		},
		func(msg []byte) {
			dispatched = append(dispatched, string(msg))
		},
	).WithBackoff(ingest.BackoffConfig{
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
	})

	err := manager.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if connects != 2 {
		t.Fatalf("connects = %d, want 2", connects)
	}
	if !first.closed {
		t.Fatal("first connection was not closed after read error")
	}
	if !second.closed {
		t.Fatal("second connection was not closed")
	}
	if len(dispatched) != 1 || dispatched[0] != `{"stream":"btcusdt@aggTrade"}` {
		t.Fatalf("dispatched = %#v, want one message from second connection", dispatched)
	}
}

func TestConnectionManagerBackoffAfterRepeatedConnectFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connects := 0
	delays := make([]time.Duration, 0, 3)

	manager := ingest.NewConnectionManager(
		func(context.Context) (ingest.MessageConnection, error) {
			connects++
			return nil, errors.New("feed down")
		},
		nil,
	).WithBackoff(ingest.BackoffConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     25 * time.Millisecond,
	}).WithSleep(func(ctx context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 3 {
			cancel()
			return ctx.Err()
		}
		return nil
	})

	err := manager.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if connects != 4 {
		t.Fatalf("connects = %d, want 4", connects)
	}

	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond}
	if len(delays) != len(want) {
		t.Fatalf("delays = %#v, want %#v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays[%d] = %v, want %v", i, delays[i], want[i])
		}
	}
}
