package ingest

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/chimaihueze/market-stream/internal/domain"
)

type envelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type rawAggTrade struct {
	Symbol       string `json:"s"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type rawMarkPrice struct {
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

type rawDepth struct {
	Symbol string     `json:"s"`
	Bids   [][]string `json:"b"`
	Asks   [][]string `json:"a"`
}

type Dispatcher struct {
	OnAggTrade  func(domain.AggTrade)
	OnMarkPrice func(domain.MarkPrice)
	OnDepth     func(domain.DepthUpdate)
}

func (d *Dispatcher) Dispatch(raw []byte) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		log.Printf("envelope parse error: %v", err)
		return
	}

	switch {
	case strings.HasSuffix(env.Stream, "@aggTrade"):
		var r rawAggTrade
		if err := json.Unmarshal(env.Data, &r); err != nil {
			log.Printf("aggTrade parse error %v", err)
			return
		}

		if d.OnAggTrade != nil {
			d.OnAggTrade(domain.AggTrade{
				Symbol:       r.Symbol,
				Price:        r.Price,
				Quantity:     r.Quantity,
				TradeTime:    r.TradeTime,
				IsBuyerMaker: r.IsBuyerMaker,
			})
		}

	case strings.Contains(env.Stream, "@markPrice"):
		var r rawMarkPrice
		if err := json.Unmarshal(env.Data, &r); err != nil {
			log.Printf("markPrice parse error %v", err)
			return
		}

		if d.OnMarkPrice != nil {
			d.OnMarkPrice(domain.MarkPrice{
				Symbol:          r.Symbol,
				MarkPrice:       r.MarkPrice,
				IndexPrice:      r.IndexPrice,
				FundingRate:     r.FundingRate,
				NextFundingTime: r.NextFundingTime,
			})
		}

	case strings.Contains(env.Stream, "@depth"):
		var r rawDepth
		if err := json.Unmarshal(env.Data, &r); err != nil {
			log.Printf("depth parse error %v", err)
			return
		}

		if d.OnDepth != nil {
			d.OnDepth(domain.DepthUpdate{
				Symbol: r.Symbol,
				Bids:   r.Bids,
				Asks:   r.Asks,
			})
		}

	}

}
