package ekanite

import (
	"context"
	"errors"
	"io"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
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

func CloseWith(closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Println("[WARN] ", err)
	}
}

func GroupBy(seacher Searcher, ctx context.Context, startAt, endAt time.Time, q query.Query, field string,
	cb func(map[string]uint64) error) error {
	dict, err := seacher.FieldDict(ctx, startAt, endAt, field)
	if err != nil {
		if err == bleve.ErrorAliasEmpty {
			return cb(map[string]uint64{})
		}
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

		// bs, _ := json.Marshal(searchRequest)
		// fmt.Println("2================================")
		// fmt.Println("1parsed request %s", string(bs))

		err := seacher.Query(ctx, startAt, endAt, searchRequest,
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

func GroupByNumeric(seacher Searcher, ctx context.Context, startAt, endAt time.Time, q query.Query, field string, start, end, step int64,
	cb func(req *bleve.SearchRequest, resp *bleve.SearchResult, results []*search.NumericRangeFacet) error) error {
	facetRequest, err := facetByNumericRange(field, start, end, step)
	if err != nil {
		return err
	}
	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.AddFacet(field, facetRequest)

	// bs, _ := json.Marshal(searchRequest)
	// fmt.Println("2================================")
	// fmt.Println("2parsed request %s", string(bs))

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			return errors.New("error validating query: " + err.Error())
		}
	}

	// execute the query
	return seacher.Query(ctx, startAt, endAt, searchRequest,
		func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
			if len(resp.Facets) == 0 {
				return errors.New("facets is empty in the search result")
			}

			count := 0
			for _, facet := range resp.Facets {
				count += len(facet.NumericRanges)
			}

			var results = make([]*search.NumericRangeFacet, 0, count)
			for _, facet := range resp.Facets {
				if results == nil {
					continue
				}

				results = append(results, facet.NumericRanges...)
			}

			sort.Slice(results, func(a, b int) bool {
				if results[a].Min == nil {
					return false
				}

				if results[b].Min == nil {
					return true
				}

				return *results[a].Min < *results[b].Min
			})
			return cb(req, resp, results)
		})
}

func GroupByTime(seacher Searcher, ctx context.Context, startAt, endAt time.Time, q query.Query, field string, value time.Duration,
	cb func(req *bleve.SearchRequest, resp *bleve.SearchResult, results []*search.DateRangeFacet) error) error {
	facetRequest, err := facetByTime(startAt, endAt, field, value)
	if err != nil {
		return err
	}
	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.AddFacet(field, facetRequest)

	// bs, _ := json.Marshal(searchRequest)
	// fmt.Println("parsed request %s", string(bs))

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			return errors.New("error validating query: " + err.Error())
		}
	}

	// execute the query
	return seacher.Query(ctx, startAt, endAt, searchRequest,
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

func facetByNumericRange(field string, start, end, interval int64) (*bleve.FacetRequest, error) {
	facetRequest := bleve.NewFacetRequest(field, math.MaxInt32)
	nextStart := start
	nextEnd := start + interval

	for nextEnd <= end {
		name := strconv.FormatInt(nextStart, 10)

		min := float64(nextStart)
		max := float64(nextEnd)

		if nextStart == start {
			facetRequest.AddNumericRange(name, nil, &max)
		} else if nextEnd == end {
			facetRequest.AddNumericRange(name, &min, nil)
		} else {
			facetRequest.AddNumericRange(name, &min, &max)
		}

		nextStart = nextEnd
		nextEnd = nextStart + interval
	}

	return facetRequest, nil
}

type errArray []error

func (ea errArray) Error() string {
	if len(ea) == 1 {
		return ea[0].Error()
	}

	var sb strings.Builder
	sb.WriteString("mult error:")
	for idx := range ea {
		sb.WriteString("\r\n\t")
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
