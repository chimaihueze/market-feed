package ingest_test

import (
	"testing"

	"github.com/chimaihueze/market-stream/internal/ingest"
)

func TestClientBuildURLUsesMarketRoute(t *testing.T) {
	client := ingest.NewMarketClient([]string{"btcusdt@aggTrade", "btcusdt@markPrice"})

	got := client.URL()
	want := "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice"

	if got != want {
		t.Fatalf("URL() = %q, want %q", got, want)
	}
}

func TestClientBuildURLUsesPublicRoute(t *testing.T) {
	client := ingest.NewPublicClient([]string{"btcusdt@depth@500ms"})

	got := client.URL()
	want := "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@500ms"

	if got != want {
		t.Fatalf("URL() = %q, want %q", got, want)
	}
}
