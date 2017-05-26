package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	"github.com/runner-mei/borm"
)

var ErrBucketNotFound = errors.New("bucket isn't found")

var OpList = []string{
	"Phrase",
	"Prefix",
	"Regexp",
	"Term",
	"Wildcard",
	"DateRange",
	"NumericRange",
	"QueryString",
}

type Filter struct {
	Field  string   `json:"field,omitempty"`
	Op     string   `json:"op"`
	Values []string `json:"values"`
}

func (f *Filter) Create() query.Query {
	switch f.Op {
	case "Phrase":
		return bleve.NewPhraseQuery(f.Values, f.Field)
	case "Prefix":
		q := bleve.NewPrefixQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case "Regexp":
		q := bleve.NewRegexpQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case "Term":
		if len(f.Values) == 0 {
			panic(errors.New("'" + f.Field + "' has invalid values"))
		}
		var queries []query.Query
		for _, v := range f.Values {
			q := bleve.NewTermQuery(v)
			q.SetField(f.Field)
			queries = append(queries, q)
		}
		return bleve.NewDisjunctionQuery(queries...)
	case "Wildcard":
		q := bleve.NewWildcardQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case "DateRange":
		var start, end time.Time
		if f.Values[0] != "" {
			start = parseTime(f.Values[0])
			if start.IsZero() {
				panic(errors.New("'" + f.Values[0] + "' is invalid datetime"))
			}
		}

		if f.Values[0] != "" {
			end = parseTime(f.Values[1])
			if end.IsZero() {
				panic(errors.New("'" + f.Values[1] + "' is invalid datetime"))
			}
		}
		inclusive := true
		q := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		q.SetField(f.Field)
		return q
	case "NumericRange":
		start, err := strconv.ParseFloat(f.Values[0], 64)
		if err != nil {
			panic(err)
		}
		end, err := strconv.ParseFloat(f.Values[1], 64)
		if err != nil {
			panic(err)
		}
		inclusive := true
		q := bleve.NewNumericRangeInclusiveQuery(&start, &end, &inclusive, &inclusive)
		q.SetField(f.Field)
		return q
	case "QueryString":
		fallthrough
	default:
		return bleve.NewQueryStringQuery(f.Values[0])
	}
}

type Query struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Filters     []Filter `json:"filters,omitempty"`
}

func (q *Query) Create() []query.Query {
	var queries = make([]query.Query, 0, len(q.Filters))
	for _, f := range q.Filters {
		queries = append(queries, f.Create())
	}
	return queries
}

func (h *HTTPServer) ListFilters(w http.ResponseWriter, r *http.Request) {
	var rs []Query
	err := h.DB.ForEach(func(it *borm.Iterator) error {
		for it.Next() {
			var q Query
			if err := it.Read(&q); err != nil {
				return err
			}
			q.ID = string(it.Key())
			rs = append(rs, q)
		}
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, rs)
}

func (h *HTTPServer) ListFilterIDs(w http.ResponseWriter, r *http.Request) {
	var rs []Query
	err := h.DB.ForEach(func(it *borm.Iterator) error {
		for it.Next() {
			var q Query
			if err := it.Read(&q); err != nil {
				return err
			}
			q.ID = string(it.Key())
			q.Filters = nil
			rs = append(rs, q)
		}
		return nil
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, rs)
}

func (h *HTTPServer) ReadFilter(w http.ResponseWriter, r *http.Request, id string) {
	var q Query
	err := h.DB.Get(id, &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, &q)
}

func (h *HTTPServer) CreateFilter(w http.ResponseWriter, r *http.Request) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	var q Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.DB.Insert(borm.GenerateID(), &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}

func (h *HTTPServer) DeleteFilter(w http.ResponseWriter, r *http.Request, id string) {
	err := h.DB.Delete(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *HTTPServer) UpdateFilter(w http.ResponseWriter, r *http.Request, id string) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	var q Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.DB.Upsert(id, &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}

func (s *HTTPServer) SummaryByFilters(w http.ResponseWriter, req *http.Request, name string) {
	var q query.Query
	if name != "0" {
		var qu Query
		if err := s.DB.Get(name, &qu); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}
		q = bleve.NewConjunctionQuery(qu.Create()...)
	}

	queryParams := req.URL.Query()
	if groupBy := queryParams.Get("group_by"); groupBy != "" {
		s.groupBy(w, req, q, queryParams, groupBy)
		return
	}

	searchRequest := bleve.NewSearchRequest(q)
	s.SearchIn(w, req, searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return encodeJSON(w, resp.Total)
	})
}

func (s *HTTPServer) SearchByFilters(w http.ResponseWriter, req *http.Request, name string) {
	var q query.Query
	if name != "0" {
		var qu Query
		if err := s.DB.Get(name, &qu); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}
		q = bleve.NewConjunctionQuery(qu.Create()...)
	}

	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.Fields = []string{"*"}

	s.SearchIn(w, req, searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		var documents = make([]interface{}, 0, resp.Hits.Len())
		for _, doc := range resp.Hits {
			documents = append(documents, doc.Fields)
		}
		return encodeJSON(w, documents)
	})
}

func (s *HTTPServer) SummaryByFiltersInBody(w http.ResponseWriter, req *http.Request) {
	var qu Query
	if err := decodeJSON(req, &qu); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	q := bleve.NewConjunctionQuery(qu.Create()...)

	searchRequest := bleve.NewSearchRequest(q)
	s.SearchIn(w, req, searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return encodeJSON(w, resp.Total)
	})
}

func (s *HTTPServer) SearchByFiltersInBody(w http.ResponseWriter, req *http.Request) {
	var qu Query
	if err := decodeJSON(req, &qu); err != nil {
		s.RenderText(w, req, http.StatusBadRequest, err.Error())
		return
	}
	q := bleve.NewConjunctionQuery(qu.Create()...)

	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.Fields = []string{"*"}

	s.SearchIn(w, req, searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		var documents = make([]interface{}, 0, resp.Hits.Len())
		for _, doc := range resp.Hits {
			documents = append(documents, doc.Fields)
		}
		return encodeJSON(w, documents)
	})
}

func (s *HTTPServer) groupBy(w http.ResponseWriter, req *http.Request, q query.Query, params url.Values, groupBy string) {
	var start, end time.Time

	startAt := params.Get("start_at")
	if startAt == "" {
		s.RenderText(w, req, http.StatusBadRequest, "start_at is missing.")
		return
	}
	start = parseTime(startAt)
	if start.IsZero() {
		s.RenderText(w, req, http.StatusBadRequest, "start_at("+startAt+") is invalid.")
		return
	}

	endAt := params.Get("end_at")
	if endAt != "" {
		end = parseTime(endAt)
		if end.IsZero() {
			s.RenderText(w, req, http.StatusBadRequest, "end_at("+endAt+") is invalid.")
			return
		}
	} else {
		end = time.Now()
	}

	inclusive := true
	timeQuery := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
	timeQuery.SetField("reception")

	if q == nil {
		q = timeQuery
	} else if conjunctionQuery, ok := q.(*query.ConjunctionQuery); ok {
		conjunctionQuery.AddQuery(timeQuery)
	} else {
		q = bleve.NewConjunctionQuery(q, timeQuery)
	}

	ss := strings.Fields(groupBy)
	switch len(ss) {
	case 1:
		s.groupByAny(w, req, q, start, end, groupBy)
	case 2:
		if ss[0] != "reception" {
			s.RenderText(w, req, http.StatusBadRequest,
				"group by("+groupBy+") is invalid format")
			return
		}
		s.groupByTimestamp(w, req, q, start, end, ss[0], ss[1])
	default:
		s.RenderText(w, req, http.StatusBadRequest,
			"group by("+groupBy+") is invalid format")
	}
}

const maxInt32 = 2147483647

func (s *HTTPServer) groupByAny(w http.ResponseWriter, req *http.Request, q query.Query, startAt, endAt time.Time, field string) {
	dict, err := s.Searcher.FieldDict(startAt, endAt, field)
	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest, "read field dictionary fail,"+err.Error())
		return
	}
	// validate the query
	if srqv, ok := q.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			s.RenderText(w, req, http.StatusBadRequest,
				fmt.Sprintf("error validating query: %v", err))
			return
		}
	}

	var results []map[string]interface{}
	for _, entry := range dict {
		var termQuery = bleve.NewTermQuery(entry.Term)
		termQuery.SetField(field)

		searchRequest := bleve.NewSearchRequest(bleve.NewConjunctionQuery(q, termQuery))

		err := s.Searcher.Query(startAt, endAt, searchRequest,
			func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
				results = append(results, map[string]interface{}{"name": entry.Term, "count": resp.Total})
				return nil
			})

		if err != nil {
			s.RenderText(w, req, http.StatusBadRequest,
				fmt.Sprintf("error executing query: %v", err))
			return
		}
	}

	encodeJSON(w, results)
}

func (s *HTTPServer) groupByTimestamp(w http.ResponseWriter, req *http.Request, q query.Query, startAt, endAt time.Time, field, value string) {
	facetRequest, err := s.facetByTime(startAt, endAt, field, value)
	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest, err.Error())
		return
	}
	searchRequest := bleve.NewSearchRequest(q)
	searchRequest.AddFacet(field, facetRequest)

	bs, _ := json.Marshal(searchRequest)
	s.Logger.Printf("parsed request %s", string(bs))

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			s.RenderText(w, req, http.StatusBadRequest,
				fmt.Sprintf("error validating query: %v", err))
			return
		}
	}

	jsBuf, _ := json.Marshal(searchRequest)
	s.Logger.Printf("parsed request %s", string(jsBuf))

	// execute the query
	err = s.Searcher.Query(startAt, endAt, searchRequest,
		func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
			if len(resp.Facets) == 0 {
				return errors.New("facets is empty in the search result.")
			}
			var results []*search.DateRangeFacet

			for _, facet := range resp.Facets {
				if results == nil {
					results = facet.DateRanges
				} else {
					results = append(results, facet.DateRanges...)
				}
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
			return encodeJSON(w, results)
		})

	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest,
			fmt.Sprintf("error executing query: %v", err))
		return
	}
}

func (s *HTTPServer) facetByTime(startAt, endAt time.Time, field, value string) (*bleve.FacetRequest, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return nil, errors.New("`" + value + "' is invalid in 'group by'.")
	}

	facetRequest := bleve.NewFacetRequest(field, maxInt32)

	nextStart := startAt
	nextEnd := startAt.Add(duration)

	for nextEnd.Before(endAt) {
		name := strconv.FormatInt(nextStart.Unix(), 10) +
			"-" +
			strconv.FormatInt(nextEnd.Unix(), 10)

		facetRequest.AddDateTimeRange(name, nextStart, nextEnd)

		nextStart = nextEnd
		nextEnd = nextStart.Add(duration)
	}

	return facetRequest, nil
}
