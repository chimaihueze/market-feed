package ingest

import "strings"

func SplitStreamsByRoute(streams []string) ([]string, []string) {
	marketStreams := make([]string, 0, len(streams))
	publicStreams := make([]string, 0, len(streams))

	for _, stream := range streams {
		if strings.Contains(stream, "@depth") {
			publicStreams = append(publicStreams, stream)
			continue
		}
		marketStreams = append(marketStreams, stream)
	}

	return marketStreams, publicStreams
}
