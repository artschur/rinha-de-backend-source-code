package internal

import (
	"fmt"
	"time"
)

func parseFlexibleTime(timeStr string) (time.Time, error) {
	if timeStr == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}

	// Formatos ISO 8601 UTC conforme especificação
	formats := []string{
		time.RFC3339,               // "2006-01-02T15:04:05Z07:00"
		"2006-01-02T15:04:05.000Z", // "2020-07-10T12:34:56.000Z"
		"2006-01-02T15:04:05Z",     // "2020-07-10T12:34:56Z"
		"2006-01-02T15:04:05",      // "2000-01-01T00:00:00" (without timezone)
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			if t.Location() == time.UTC || format == "2006-01-02T15:04:05" || format == "2006-01-02" {
				return t.UTC(), nil
			}
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid ISO UTC date format '%s' (expected: 2020-07-10T12:34:56.000Z)", timeStr)
}
