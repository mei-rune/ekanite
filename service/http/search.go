package http

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite/service"
)

func (s *Server) SummaryByFilters(w http.ResponseWriter, req *http.Request, name string) {
	var q query.Query
	if name != "0" && name != "" {
		var qu, err = s.metaStore.ReadQuery(name)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}
		queries, err := qu.ToQueries()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}

		q = bleve.NewConjunctionQuery(queries...)
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

func (s *Server) SearchByFilters(w http.ResponseWriter, req *http.Request, name string) {
	var q query.Query
	if name != "0" && name != "" {
		var qu, err = s.metaStore.ReadQuery(name)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}

		queries, err := qu.ToQueries()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}
		q = bleve.NewConjunctionQuery(queries...)
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

func (s *Server) SummaryByFiltersInBody(w http.ResponseWriter, req *http.Request) {
	var qu service.Query
	if err := decodeJSON(req, &qu); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	queries, err := qu.ToQueries()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bucket: " + err.Error()))
		return
	}

	q := bleve.NewConjunctionQuery(queries...)

	searchRequest := bleve.NewSearchRequest(q)
	s.SearchIn(w, req, searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return encodeJSON(w, resp.Total)
	})
}

func (s *Server) SearchByFiltersInBody(w http.ResponseWriter, req *http.Request) {
	var qu service.Query
	if err := decodeJSON(req, &qu); err != nil {
		s.RenderText(w, req, http.StatusBadRequest, err.Error())
		return
	}

	queries, err := qu.ToQueries()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bucket: " + err.Error()))
		return
	}

	q := bleve.NewConjunctionQuery(queries...)

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

func (s *Server) groupBy(w http.ResponseWriter, req *http.Request, q query.Query, params url.Values, groupBy string) {
	var start, end time.Time

	startAt := params.Get("start_at")
	if startAt == "" {
		s.RenderText(w, req, http.StatusBadRequest, "start_at is missing.")
		return
	}
	start = service.ParseTime(startAt)
	if start.IsZero() {
		s.RenderText(w, req, http.StatusBadRequest, "start_at("+startAt+") is invalid.")
		return
	}

	endAt := params.Get("end_at")
	if endAt != "" {
		end = service.ParseTime(endAt)
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
		if ss[0] == "reception" {
			s.RenderText(w, req, http.StatusBadRequest,
				"group by("+groupBy+") is invalid format")
			return
		}
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

func (s *Server) groupByAny(w http.ResponseWriter, req *http.Request, q query.Query, startAt, endAt time.Time, field string) {
	var results []map[string]interface{}
	err := service.GroupBy(s.Searcher, startAt, endAt, q, field, func(stats map[string]uint64) error {
		for key, value := range stats {
			results = append(results, map[string]interface{}{"name": key, "count": value})
		}
		return nil
	})

	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest, err.Error())
		return
	}
	renderJSON(w, results)
}

func (s *Server) groupByTimestamp(w http.ResponseWriter, req *http.Request, q query.Query, startAt, endAt time.Time, field, value string) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest,
			"error executing query: `"+value+"' is invalid in 'group by'")

		return
	}

	err = service.GroupByTime(s.Searcher, startAt, endAt, q, field, duration,
		func(req *bleve.SearchRequest, resp *bleve.SearchResult, results []*search.DateRangeFacet) error {
			return encodeJSON(w, results)
		})
	if err != nil {
		s.RenderText(w, req, http.StatusBadRequest,
			fmt.Sprintf("error executing query: %v", err))
		return
	}
}
