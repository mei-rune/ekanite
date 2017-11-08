package service

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var (
	timeFormats = []string{
		"2006-01-02T15:04:05.000Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02T15:04:05.999999999 07:00",
		"2006-01-02T15:04:05 07:00"}
)

func ParseTime(s string) time.Time {
	for _, layout := range timeFormats {
		v, err := time.ParseInLocation(layout, s, time.Local)
		if err == nil {
			return v.Local()
		}
	}

	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "now()") {
		durationStr := strings.TrimSpace(strings.TrimPrefix(s, "now()"))
		if durationStr == "" {
			return time.Now()
		}
		neg := false
		if strings.HasPrefix(durationStr, "-") {
			neg = true
			durationStr = strings.TrimSpace(strings.TrimPrefix(durationStr, "-"))
		}

		duration, err := time.ParseDuration(durationStr)
		if err == nil {
			if neg {
				duration = -1 * duration
			}
			return time.Now().Add(duration)
		}
	}
	return time.Time{}
}

func readFromFile(file string, value interface{}) error {
	in, err := os.Open(file)
	if err != nil {
		return err
	}
	defer CloseWith(in)

	decoder := json.NewDecoder(in)
	return decoder.Decode(value)
}

func writeToFile(file string, value interface{}) error {
	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer CloseWith(out)

	decoder := json.NewEncoder(out)
	return decoder.Encode(value)
}

func CloseWith(closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Println("[WARN] ", err)
	}
}
