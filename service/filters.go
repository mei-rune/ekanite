package service

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
)

// var ErrBucketNotFound = errors.New("bucket isn't found")

const (
	OpPhrase       = "Phrase"
	OpPrefix       = "Prefix"
	OpRegexp       = "Regexp"
	OpTerm         = "Term"
	OpWildcard     = "Wildcard"
	OpDateRange    = "DateRange"
	OpNumericRange = "NumericRange"
	OpQueryString  = "QueryString"
)

var OpList = []string{
	OpPhrase,
	OpPrefix,
	OpRegexp,
	OpTerm,
	OpWildcard,
	OpDateRange,
	OpNumericRange,
	OpQueryString,
}

// Filter 过滤器
type Filter struct {
	Field  string   `json:"field,omitempty"`
	Op     string   `json:"op"`
	Values []string `json:"values"`
}

// ToQuery 转换 query.Query
func (f *Filter) ToQuery() query.Query {
	switch f.Op {
	case OpPhrase:
		return bleve.NewPhraseQuery(f.Values, f.Field)
	case OpPrefix:
		q := bleve.NewPrefixQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case OpRegexp:
		q := bleve.NewRegexpQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case OpTerm:
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
	case OpWildcard:
		q := bleve.NewWildcardQuery(f.Values[0])
		q.SetField(f.Field)
		return q
	case OpDateRange:
		var start, end time.Time
		if f.Values[0] != "" {
			start = ParseTime(f.Values[0])
			if start.IsZero() {
				panic(errors.New("'" + f.Values[0] + "' is invalid datetime"))
			}
		}

		if f.Values[0] != "" {
			end = ParseTime(f.Values[1])
			if end.IsZero() {
				panic(errors.New("'" + f.Values[1] + "' is invalid datetime"))
			}
		}
		inclusive := true
		q := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		q.SetField(f.Field)
		return q
	case OpNumericRange:
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
	case OpQueryString:
		fallthrough
	default:
		return bleve.NewQueryStringQuery(f.Values[0])
	}
}

// Query 一个查询对象
type Query struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Filters     []Filter `json:"filters,omitempty"`
}

func (q *Query) ToQueries() []query.Query {
	var queries = make([]query.Query, 0, len(q.Filters))
	for _, f := range q.Filters {
		queries = append(queries, f.ToQuery())
	}
	return queries
}

type QueryStore struct {
	dataPath string
}

func (h *QueryStore) List() ([]Query, error) {
	files, err := ioutil.ReadDir(h.dataPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return nil, nil
	}

	var rs []Query
	for _, file := range files {
		var q Query
		if err := readFromFile(filepath.Join(h.dataPath, file.Name()), &q); err != nil {
			return nil, err
		}

		q.ID = strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		rs = append(rs, q)
	}
	return rs, nil
}

func (h *QueryStore) IDs() ([]Query, error) {
	files, err := ioutil.ReadDir(h.dataPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return nil, nil
	}

	var rs []Query
	for _, file := range files {
		var q Query
		if err := readFromFile(filepath.Join(h.dataPath, file.Name()), &q); err != nil {
			return nil, err
		}
		q.ID = strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		q.Filters = nil
		rs = append(rs, q)
	}
	return rs, nil
}

func (h *QueryStore) Read(id string) (*Query, error) {
	var q Query
	err := readFromFile(filepath.Join(h.dataPath, id+".json"), &q)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func (h *QueryStore) Create(q Query) error {
	return writeToFile(filepath.Join(h.dataPath, GenerateID()+".json"), &q)
}

func (h *QueryStore) Delete(id string) error {
	err := os.Remove(filepath.Join(h.dataPath, id+".json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func (h *QueryStore) Write(id string, q Query) error {
	return writeToFile(filepath.Join(h.dataPath, id+".json"), &q)
}
