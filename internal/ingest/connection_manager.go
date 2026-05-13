package ingest

import (
	"context"
	"log"
	"math/rand"
	"time"
)

type MessageConnection interface {
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

type ConnectFunc func(context.Context) (MessageConnection, error)

type DispatchFunc func([]byte)

type SleepFunc func(context.Context, time.Duration) error

type BackoffConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Jitter       float64
}

type ConnectionManager struct {
	connect  ConnectFunc
	dispatch DispatchFunc
	sleep    SleepFunc
	backoff  BackoffConfig
	jitter   func() float64
}

func NewConnectionManager(connect ConnectFunc, dispatch DispatchFunc) *ConnectionManager {
	return &ConnectionManager{
		connect:  connect,
		dispatch: dispatch,
		sleep:    sleepContext,
		backoff: BackoffConfig{
			InitialDelay: time.Second,
			MaxDelay:     30 * time.Second,
			Jitter:       0.2,
		},
		jitter: rand.Float64,
	}
}

func (m *ConnectionManager) WithBackoff(backoff BackoffConfig) *ConnectionManager {
	m.backoff = backoff
	return m
}

func (m *ConnectionManager) WithSleep(sleep SleepFunc) *ConnectionManager {
	m.sleep = sleep
	return m
}

func (m *ConnectionManager) WithJitter(jitter func() float64) *ConnectionManager {
	m.jitter = jitter
	return m
}

func (m *ConnectionManager) Run(ctx context.Context) error {
	failures := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.runOnce(ctx, &failures); err != nil {
			return err
		}
	}
}

func (m *ConnectionManager) runOnce(ctx context.Context, failures *int) error {
	conn, err := m.connect(ctx)
	if err != nil {
		log.Printf("[ingest] connect error: %v", err)
		if err := m.waitBeforeRetry(ctx, *failures); err != nil {
			return err
		}
		*failures++
		return nil
	}

	readErr := m.readAndDispatch(conn, failures)
	if closeErr := conn.Close(); closeErr != nil {
		log.Printf("[ingest] close error: %v", closeErr)
	}

	if readErr != nil {
		log.Printf("[ingest] read error: %v", readErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := m.waitBeforeRetry(ctx, *failures); err != nil {
		return err
	}
	*failures++
	return nil
}

func (m *ConnectionManager) readAndDispatch(conn MessageConnection, failures *int) error {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		*failures = 0
		if m.dispatch != nil {
			m.dispatch(msg)
		}
	}
}

func (m *ConnectionManager) waitBeforeRetry(ctx context.Context, failures int) error {
	delay := m.backoff.Delay(failures, m.jitter)
	if delay <= 0 {
		return nil
	}
	return m.sleep(ctx, delay)
}

func (b BackoffConfig) Delay(failures int, jitter func() float64) time.Duration {
	if failures <= 0 {
		return 0
	}

	initial := b.InitialDelay
	if initial <= 0 {
		initial = time.Second
	}
	maxDelay := b.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}

	delay := initial
	for i := 1; i < failures; i++ {
		delay *= 2
		if delay >= maxDelay {
			delay = maxDelay
			break
		}
	}

	if b.Jitter > 0 && jitter != nil {
		spread := 1 + ((jitter()*2)-1)*b.Jitter
		delay = time.Duration(float64(delay) * spread)
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
