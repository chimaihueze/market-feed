package ingest_test

import (
	"testing"

	"github.com/chimaihueze/market-stream/internal/ingest"
)

func TestSplitStreamsByRoute(t *testing.T) {
	streams := []string{
		"btcusdt@aggTrade",
		"btcusdt@markPrice",
		"btcusdt@depth@500ms",
	}

	marketStreams, publicStreams := ingest.SplitStreamsByRoute(streams)

	if len(marketStreams) != 2 {
		t.Fatalf("marketStreams length = %d, want 2", len(marketStreams))
	}
	if len(publicStreams) != 1 {
		t.Fatalf("publicStreams length = %d, want 1", len(publicStreams))
	}
	if publicStreams[0] != "btcusdt@depth@500ms" {
		t.Fatalf("publicStreams[0] = %q, want depth stream", publicStreams[0])
	}
}
