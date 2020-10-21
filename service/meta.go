package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
)

// 一些常量
const (
	OpMatch  = "Match"
	OpPhrase = "Phrase"
	// OpMultiPhrase  = "MultiPhrase"
	OpPrefix       = "Prefix"
	OpRegexp       = "Regexp"
	OpTerm         = "Term"
	OpWildcard     = "Wildcard"
	OpDateRange    = "DateRange"
	OpNumericRange = "NumericRange"
	OpQueryString  = "QueryString"

	QueryObject = "query.json"
)

// 一些常见错误
var (
	ErrRecordNotFound = errors.New("record isnot found")
	ErrNameIsExists   = errors.New("query name is exists")
)

// OpList 一些过滤表达式的匹配操作
var OpList = []string{
	OpMatch,
	OpPhrase,
	// OpMultiPhrase,
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

type errBadArguments struct {
	msg string
}

func (e errBadArguments) Error() string {
	return e.msg
}

func ErrBadArguments(msg string) error {
	return errBadArguments{msg: msg}
}

// ToQuery 转换为 query.Query
func (f *Filter) ToQuery() (query.Query, error) {
	switch f.Op {
	case OpMatch:
		if f.Values[0] == "" {
			return nil, ErrBadArguments("query is empty")
		}
		q := bleve.NewMatchQuery(f.Values[0])
		q.SetField(f.Field)
		return q, nil
	case OpPhrase:
		return bleve.NewPhraseQuery(f.Values, f.Field), nil
	// case OpMultiPhrase:
	// 	return bleve.NewMultiPhraseQuery(f.Values, f.Field), nil
	case OpPrefix:
		if f.Values[0] == "" {
			return nil, ErrBadArguments("prefixQuery is empty")
		}

		q := bleve.NewPrefixQuery(f.Values[0])
		q.SetField(f.Field)
		return q, nil
	case OpRegexp:
		if f.Values[0] == "" {
			return nil, ErrBadArguments("regexpQuery is empty")
		}

		q := bleve.NewRegexpQuery(f.Values[0])
		q.SetField(f.Field)
		return q, nil
	case OpTerm:
		if len(f.Values) == 0 {
			return nil, errors.New("'" + f.Field + "' has invalid values")
		}
		var queries []query.Query
		for _, v := range f.Values {
			if v == "" {
				return nil, errors.New("'" + f.Field + "' has empty value")
			}

			q := bleve.NewTermQuery(v)
			q.SetField(f.Field)
			queries = append(queries, q)
		}
		return bleve.NewDisjunctionQuery(queries...), nil
	case OpWildcard:
		if f.Values[0] == "" {
			return nil, ErrBadArguments("wildcardQuery is empty")
		}
		q := bleve.NewWildcardQuery(f.Values[0])
		q.SetField(f.Field)
		return q, nil
	case OpDateRange:
		var start, end time.Time
		if f.Values[0] != "" {
			start = ekanite.ParseTime(f.Values[0])
			if start.IsZero() {
				return nil, errors.New("'" + f.Values[0] + "' is invalid datetime")
			}
		}

		if f.Values[0] != "" {
			end = ekanite.ParseTime(f.Values[1])
			if end.IsZero() {
				return nil, errors.New("'" + f.Values[1] + "' is invalid datetime")
			}
		}
		inclusive := true
		q := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		q.SetField(f.Field)
		return q, nil
	case OpNumericRange:
		start, err := strconv.ParseFloat(f.Values[0], 64)
		if err != nil {
			return nil, err
		}
		end, err := strconv.ParseFloat(f.Values[1], 64)
		if err != nil {
			return nil, err
		}
		inclusive := true
		if math.IsNaN(start) || math.IsInf(start, 0) {
			if math.IsNaN(end) || math.IsInf(end, 0) {
				return nil, fmt.Errorf("NumericRange(%v) is invalid", f.Values)
			}

			q := bleve.NewNumericRangeInclusiveQuery(nil, &end, nil, &inclusive)
			q.SetField(f.Field)
			return q, nil
		}
		if math.IsNaN(end) || math.IsInf(end, 0) {
			q := bleve.NewNumericRangeInclusiveQuery(&start, nil, &inclusive, nil)
			q.SetField(f.Field)
			return q, nil
		}

		q := bleve.NewNumericRangeInclusiveQuery(&start, &end, &inclusive, &inclusive)
		q.SetField(f.Field)
		return q, nil
	case OpQueryString:
		fallthrough
	default:
		if f.Values[0] == "" {
			return nil, ErrBadArguments("query is empty")
		}
		return bleve.NewQueryStringQuery(f.Values[0]), nil
	}
}

// ContinuousQuery 一个持续查询对象
type ContinuousQuery struct {
	Fields  []string `json:"fields,omitempty"`
	GroupBy string   `json:"groupBy,omitempty"`
	Targets []struct {
		Type      string   `json:"type"`
		Arguments []string `json:"arguments"`
	} `json:"targets,omitempty"`

	//  cache for target callback
	Callback func(cq *ContinuousQuery, value interface{}) error `json:"-"`
}

// Query 一个查询对象
type Query struct {
	ID                string                     `json:"id,omitempty"`
	Name              string                     `json:"name"`
	Description       string                     `json:"description,omitempty"`
	Filters           []Filter                   `json:"filters,omitempty"`
	ContinuousQueries map[string]ContinuousQuery `json:"continuous_queries,omitempty"`
	Sort              string                     `json:"sort,omitempty"`
}

// ToQueries 转换为 query.Query 列表
func (q *Query) ToQueries() ([]query.Query, error) {
	var queries = make([]query.Query, 0, len(q.Filters))
	for _, f := range q.Filters {
		if len(f.Field) == 0 {
			continue
		}
		if len(f.Op) == 0 {
			continue
		}
		if len(f.Values) == 0 {
			continue
		}

		if f.Values[0] == "" {
			continue
		}

		query, err := f.ToQuery()
		if err != nil {
			return nil, err
		}

		queries = append(queries, query)
	}
	return queries, nil
}

func NewMetaStore(dataPath string) *MetaStore {
	return &MetaStore{dataPath: dataPath, backupCount: 5}
}

// MetaStore 对象
type MetaStore struct {
	dataPath    string
	backupCount int
	mu          sync.RWMutex
	queries     map[string]Query
}

func (h *MetaStore) Load() error {
	var queries map[string]Query
	filename := filepath.Join(h.dataPath, "meta.json")

	if err := readFromFile(filename, &queries); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		return nil
	}

	h.mu.Lock()
	h.queries = queries
	h.mu.Unlock()
	return nil
}

func (h *MetaStore) save() error {
	filename := filepath.Join(h.dataPath, "meta.json")

	if err := os.MkdirAll(filepath.Dir(filename), 0666); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	if err := writeToFile(filename+".tmp", &h.queries); err != nil {
		return err
	}

	backupIdx := h.backupCount
	if backupIdx == 0 {
		backupIdx = 5
	}
	if err := os.Remove(filename + "." + strconv.Itoa(backupIdx)); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	for ; backupIdx > 0; backupIdx-- {
		err := os.Rename(filename+"."+strconv.Itoa(backupIdx),
			filename+"."+strconv.Itoa(backupIdx+1))
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	if err := os.Rename(filename+".0", filename); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(filename+".tmp", filename)
}

func (h *MetaStore) ForEach(cb func(id string, data Query)) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.queries == nil {
		return
	}

	for k, v := range h.queries {
		cb(k, v)
	}
}

func (h *MetaStore) ListQueries() []Query {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.queries == nil {
		return nil
	}

	var list []Query
	for k, v := range h.queries {
		list = append(list, v)
		vv := &list[len(list)-1]
		vv.ID = k
	}
	return list
}

func (h *MetaStore) ListQueryIDs() ([]Query, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.queries == nil {
		return nil, nil
	}

	var list []Query
	for k, v := range h.queries {
		list = append(list, v)
		vv := &list[len(list)-1]

		vv.ID = k
		vv.Filters = nil
		vv.ContinuousQueries = nil
	}
	return list, nil
}

func (h *MetaStore) ReadQuery(id string) (Query, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.queries == nil {
		return Query{}, ErrRecordNotFound
	}

	q, ok := h.queries[id]
	if !ok {
		return Query{}, ErrRecordNotFound
	}
	q.ID = id
	return q, nil
}

func (h *MetaStore) CreateQuery(q Query) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.queries == nil {
		h.queries = map[string]Query{}
	}

	for _, v := range h.queries {
		if v.Name == q.Name {
			return "", errors.New("query name is exists")
		}
	}

	id := GenerateID()
	h.queries[id] = q
	return id, h.save()
}

func (h *MetaStore) DeleteQuery(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.queries) == 0 {
		return nil
	}
	if _, ok := h.queries[id]; ok {
		delete(h.queries, id)
		return h.save()
	}
	return nil
}

func (h *MetaStore) UpdateQuery(id string, q Query) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.queries) == 0 {
		return ErrRecordNotFound
	}

	_, ok := h.queries[id]
	if !ok {
		return ErrRecordNotFound
	}

	for key, v := range h.queries {
		if v.Name == q.Name && id != key {
			return ErrNameIsExists
		}
	}

	//old = q
	h.queries[id] = q
	return h.save()
}

func (h *MetaStore) ListCQ(query string) ([]ContinuousQuery, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.queries) == 0 {
		return nil, ErrRecordNotFound
	}

	q, ok := h.queries[query]
	if !ok {
		return nil, ErrRecordNotFound
	}
	if len(q.ContinuousQueries) == 0 {
		return nil, nil
	}

	var list = make([]ContinuousQuery, 0, len(q.ContinuousQueries))
	for _, cq := range q.ContinuousQueries {
		//cq.ID = key
		list = append(list, cq)
	}
	return list, nil
}

func (h *MetaStore) ReadCQ(query, id string) (ContinuousQuery, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.queries) == 0 {
		return ContinuousQuery{}, ErrRecordNotFound
	}

	q, ok := h.queries[query]
	if !ok {
		return ContinuousQuery{}, ErrRecordNotFound
	}
	if q.ContinuousQueries == nil {
		return ContinuousQuery{}, ErrRecordNotFound
	}
	cq, ok := q.ContinuousQueries[id]
	if !ok {
		return ContinuousQuery{}, ErrRecordNotFound
	}
	//cq.ID = id
	return cq, nil
}

func (h *MetaStore) CreateCQ(query string, cq ContinuousQuery) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.queries) == 0 {
		return "", ErrRecordNotFound
	}

	q, ok := h.queries[query]
	if !ok {
		return "", ErrRecordNotFound
	}
	if q.ContinuousQueries == nil {
		q.ContinuousQueries = map[string]ContinuousQuery{}
	}

	id := GenerateID()
	q.ContinuousQueries[id] = cq
	h.queries[id] = q
	return id, h.save()
}

func (h *MetaStore) DeleteCQ(query, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.queries) == 0 {
		return ErrRecordNotFound
	}

	q, ok := h.queries[query]
	if !ok {
		return ErrRecordNotFound
	}
	if q.ContinuousQueries == nil {
		return nil
	}
	delete(q.ContinuousQueries, id)
	h.queries[query] = q
	return h.save()
}

func (h *MetaStore) UpdateCQ(query, id string, cq ContinuousQuery) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.queries) == 0 {
		return ErrRecordNotFound
	}

	q, ok := h.queries[query]
	if !ok {
		return ErrRecordNotFound
	}
	if q.ContinuousQueries == nil {
		return ErrRecordNotFound
	}

	if _, ok := q.ContinuousQueries[id]; !ok {
		return ErrRecordNotFound
	}
	q.ContinuousQueries[id] = cq
	h.queries[query] = q
	return h.save()
}

func readFromFile(file string, value interface{}) error {
	in, err := os.Open(file)
	if err != nil {
		return err
	}
	defer ekanite.CloseWith(in)

	decoder := json.NewDecoder(in)
	return decoder.Decode(value)
}

func writeToFile(file string, value interface{}) error {
	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer ekanite.CloseWith(out)

	decoder := json.NewEncoder(out)
	return decoder.Encode(value)
}
