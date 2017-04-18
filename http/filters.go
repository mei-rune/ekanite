package http

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/blevesearch/bleve"
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
		q := bleve.NewTermQuery(f.Values[0])
		q.SetField(f.Field)
		return q
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
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("Bucket: " + err.Error()))
			return
		}
		q = bleve.NewConjunctionQuery(qu.Create()...)
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
			w.WriteHeader(http.StatusMethodNotAllowed)
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
