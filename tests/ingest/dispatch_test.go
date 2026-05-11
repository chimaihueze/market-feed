package ingest_test

import (
	"testing"

	"github.com/chimaihueze/market-stream/internal/domain"
	"github.com/chimaihueze/market-stream/internal/ingest"
)

func TestDispatchAggTrade(t *testing.T) {
	called := false
	dispatcher := ingest.Dispatcher{
		OnAggTrade: func(trade domain.AggTrade) {
			called = true
			if trade.Symbol != "BTCUSDT" || trade.Price != "100.1" || trade.Quantity != "0.2" {
				t.Fatalf("unexpected trade: %#v", trade)
			}
		},
	}

	dispatcher.Dispatch([]byte(`{"stream":"btcusdt@aggTrade","data":{"s":"BTCUSDT","p":"100.1","q":"0.2","T":123,"m":true}}`))

	if !called {
		t.Fatal("OnAggTrade was not called")
	}
}

func TestDispatchMarkPrice(t *testing.T) {
	called := false
	dispatcher := ingest.Dispatcher{
		OnMarkPrice: func(mark domain.MarkPrice) {
			called = true
			if mark.Symbol != "BTCUSDT" || mark.MarkPrice != "100.1" || mark.IndexPrice != "100.2" {
				t.Fatalf("unexpected mark price: %#v", mark)
			}
		},
	}

	dispatcher.Dispatch([]byte(`{"stream":"btcusdt@markPrice","data":{"s":"BTCUSDT","p":"100.1","i":"100.2","r":"0.01","T":123}}`))

	if !called {
		t.Fatal("OnMarkPrice was not called")
	}
}

func TestDispatchDepthWithUpdateSpeedSuffix(t *testing.T) {
	called := false
	dispatcher := ingest.Dispatcher{
		OnDepth: func(depth domain.DepthUpdate) {
			called = true
			if depth.Symbol != "BTCUSDT" || len(depth.Bids) != 1 || len(depth.Asks) != 1 {
				t.Fatalf("unexpected depth update: %#v", depth)
			}
		},
	}

	dispatcher.Dispatch([]byte(`{"stream":"btcusdt@depth@500ms","data":{"s":"BTCUSDT","b":[["100.1","1"]],"a":[["100.2","2"]]}}`))

	if !called {
		t.Fatal("OnDepth was not called")
	}
}
