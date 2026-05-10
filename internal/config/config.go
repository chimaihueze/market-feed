package config

import "os"

type Config struct {
	Symbols []string
	Streams []string
}

func Load() Config {
	symbols := []string{"btcusdt", "ethusdt"}

	if s := os.Getenv("SYMBOLS"); s != "" {
		symbols = splitCSV(s)
	}

	streams := make([]string, 0, len(symbols)*3)

	for _, symbol := range symbols {
		streams = append(streams,
			symbol+"@aggTrade",
			symbol+"@markPrice",
			symbol+"@depth@500ms",
		)
	}

	return Config{symbols, streams}
}

func splitCSV(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if token := s[start:i]; token != "" {
				result = append(result, token)
			}
			start = i + 1
		}
	}
	if token := s[start:]; token != "" {
		result = append(result, token)
	}
	return result
}
