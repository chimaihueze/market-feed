package ingest

type Client struct {
	streams []string
}

func NewClient(streams []string) *Client {
	return &Client{streams: streams}
}
