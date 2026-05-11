package main

import (
	"context"
	"fmt"
	"log"

	"github.com/chimaihueze/market-stream/internal/config"
	"github.com/chimaihueze/market-stream/internal/domain"
	"github.com/chimaihueze/market-stream/internal/ingest"
)

func main() {
	fmt.Println("Ingestor starting...")

	ctx := context.Background()
	cfg := config.Load()

	dispatcher := &ingest.Dispatcher{
		OnAggTrade: func(trade domain.AggTrade) {
			fmt.Printf("%s price=%s qty=%s\n", trade.Symbol, trade.Price, trade.Quantity)
		},
		OnMarkPrice: func(mark domain.MarkPrice) {
			fmt.Printf("%s mark=%s index=%s funding=%s\n", mark.Symbol, mark.MarkPrice, mark.IndexPrice, mark.FundingRate)
		},
		OnDepth: func(depth domain.DepthUpdate) {
			fmt.Printf("%s depth bids=%d asks=%d\n", depth.Symbol, len(depth.Bids), len(depth.Asks))
		},
	}

	marketStreams, publicStreams := ingest.SplitStreamsByRoute(cfg.Streams)
	errs := make(chan error, 2)

	go readStream(ctx, ingest.NewMarketClient(marketStreams), dispatcher, errs)
	go readStream(ctx, ingest.NewPublicClient(publicStreams), dispatcher, errs)

	log.Fatal(<-errs)
}

func readStream(ctx context.Context, client *ingest.Client, dispatcher *ingest.Dispatcher, errs chan<- error) {
	conn, err := client.Connect(ctx)

	if err != nil {
		errs <- err
		return
	}
	defer conn.Close()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			errs <- err
			return
		}
		dispatcher.Dispatch(msg)
	}
}
