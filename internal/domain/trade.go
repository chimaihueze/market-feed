package domain

type AggTrade struct {
	Symbol       string
	Price        string
	Quantity     string
	TradeTime    int64
	IsBuyerMaker bool
}

type MarkPrice struct {
	Symbol          string
	MarkPrice       string
	IndexPrice      string
	FundingRate     string
	NextFundingTime int64
}

type DepthUpdate struct {
	Symbol string
	Bids   [][]string
	Asks   [][]string
}
