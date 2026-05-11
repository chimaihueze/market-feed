package ingest

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/gorilla/websocket"
)

type Client struct {
	streams []string
	route   string
}

func NewClient(streams []string) *Client {
	return NewMarketClient(streams)
}

func NewMarketClient(streams []string) *Client {
	return &Client{streams: streams, route: "market"}
}

func NewPublicClient(streams []string) *Client {
	return &Client{streams: streams, route: "public"}
}

func (c *Client) URL() string {
	return c.buildURL()
}

func (c *Client) buildURL() string {
	var combined strings.Builder
	for i, s := range c.streams {
		if i > 0 {
			combined .WriteString("/")
		}
		combined .WriteString(s)
	}

	route := strings.Trim(c.route, "/")
	if route == "" {
		route = "market"
	}

	u := url.URL{
		Scheme:   "wss",
		Host:     "fstream.binance.com",
		Path:     "/" + route + "/stream",
		RawQuery: "streams=" + combined.String(),
	}
	return u.String()
}

func (c *Client) Connect(ctx context.Context) (*websocket.Conn, error) {
	wsURL := c.buildURL()
	log.Printf("Connecting to %s", wsURL)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)

	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	return conn, nil
}

func (c *Client) ReadRaw(ctx context.Context, conn *websocket.Conn) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[ingest] read error: %v", err)
			return
		}
		fmt.Println(string(msg))
	}
}
