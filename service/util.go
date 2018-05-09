package service

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
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

func GroupBy(seacher ekanite.Searcher, startAt, endAt time.Time, q query.Query, field string,
	cb func(map[string]uint64) error) error {
	dict, err := seacher.FieldDict(startAt, endAt, field)
	if err != nil {
		return errors.New("read field dictionary fail," + err.Error())
	}

	// validate the query
	if srqv, ok := q.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			return errors.New("error validating query: " + err.Error())
		}
	}

	var stats = map[string]uint64{}
	for _, entry := range dict {
		var termQuery = bleve.NewTermQuery(entry.Term)
		termQuery.SetField(field)

		searchRequest := bleve.NewSearchRequest(bleve.NewConjunctionQuery(q, termQuery))
		err := seacher.Query(startAt, endAt, searchRequest,
			func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
				stats[entry.Term] = resp.Total
				return nil
			})
		if err != nil {
			return errors.New("error executing query: " + err.Error())
		}
	}

	return cb(stats)
}

func GroupByTime(seacher ekanite.Searcher, startAt, endAt time.Time, q query.Query, field string, value time.Duration,
	cb func(req *bleve.SearchRequest, resp *bleve.SearchResult, results []*search.DateRangeFacet) error) error {
	facetRequest, err := facetByTime(startAt, endAt, field, value)
	if err != nil {
		return err
	}
	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.AddFacet(field, facetRequest)

	//bs, _ := json.Marshal(searchRequest)
	//s.Logger.Printf("parsed request %s", string(bs))

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			return errors.New("error validating query: " + err.Error())
		}
	}

	// execute the query
	return seacher.Query(startAt, endAt, searchRequest,
		func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
			if len(resp.Facets) == 0 {
				return errors.New("facets is empty in the search result")
			}

			count := 0
			for _, facet := range resp.Facets {
				count += len(facet.DateRanges)
			}

			var results = make([]*search.DateRangeFacet, 0, count)
			for _, facet := range resp.Facets {
				if results == nil {
					continue
				}

				results = append(results, facet.DateRanges...)
			}

			sort.Slice(results, func(a, b int) bool {
				if results[a].Start == nil {
					return false
				}

				if results[b].Start == nil {
					return true
				}

				return strings.Compare(*results[a].Start, *results[b].Start) < 0
			})
			return cb(req, resp, results)
		})
}

func facetByTime(startAt, endAt time.Time, field string, value time.Duration) (*bleve.FacetRequest, error) {
	facetRequest := bleve.NewFacetRequest(field, math.MaxInt32)

	nextStart := startAt
	nextEnd := startAt.Add(value)

	for nextEnd.Before(endAt) {
		name := strconv.FormatInt(nextStart.Unix(), 10) +
			"-" +
			strconv.FormatInt(nextEnd.Unix(), 10)

		facetRequest.AddDateTimeRange(name, nextStart, nextEnd)

		nextStart = nextEnd
		nextEnd = nextStart.Add(value)
	}

	return facetRequest, nil
}

type errArray []error

func (ea errArray) Error() string {
	var sb strings.Builder
	for idx := range ea {
		if idx > 0 {
			sb.WriteString("\r\n\t")
		}
		sb.WriteString(ea[idx].Error())
	}
	return sb.String()
}

func ErrArray(errList []error) error {
	if len(errList) == 0 {
		return nil
	}

	return errArray(errList)
}

func AlignTime(t time.Time, interval time.Duration) time.Time {
	unixTime := (t.Unix() / int64(interval.Seconds())) * int64(interval.Seconds())
	now := time.Unix(unixTime, 0)
	for now.Before(t) {
		now = now.Add(interval)
	}
	return now
}
