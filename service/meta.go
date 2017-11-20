package service

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
)

// 一些常量
const (
	OpPhrase       = "Phrase"
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

// ToQuery 转换为 query.Query
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

// ContinuousQuery 一个持续查询对象
type ContinuousQuery struct {
}

// Query 一个查询对象
type Query struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Filters     []Filter `json:"filters,omitempty"`
}

// ToQueries 转换为 query.Query 列表
func (q *Query) ToQueries() []query.Query {
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
		queries = append(queries, f.ToQuery())
	}
	return queries
}

// QueryData Query 及相关数据的存储对象
type QueryData struct {
	Query Query
	CQ    map[string]ContinuousQuery `json:"continuous_queries,omitempty"`
}

func NewMetaStore(dataPath string) *MetaStore {
	return &MetaStore{dataPath: dataPath, backupCount: 5}
}

// MetaStore 对象
type MetaStore struct {
	dataPath    string
	backupCount int
	mu          sync.RWMutex
	queries     map[string]QueryData
}

func (h *MetaStore) Load() error {
	var queries map[string]QueryData
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

func (h *MetaStore) ForEach(cb func(id string, data QueryData)) {
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
		v.Query.ID = k
		list = append(list, v.Query)
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
		v.Query.ID = k
		v.Query.Filters = nil
		v.CQ = nil
		list = append(list, v.Query)
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
	q.Query.ID = id
	return q.Query, nil
}

func (h *MetaStore) CreateQuery(q Query) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.queries == nil {
		h.queries = map[string]QueryData{}
	}

	for _, v := range h.queries {
		if v.Query.Name == q.Name {
			return "", errors.New("query name is exists")
		}
	}

	id := GenerateID()
	h.queries[id] = QueryData{
		Query: q,
	}
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

	old, ok := h.queries[id]
	if !ok {
		return ErrRecordNotFound
	}

	for key, v := range h.queries {
		if v.Query.Name == q.Name && id != key {
			return ErrNameIsExists
		}
	}

	old.Query = q
	h.queries[id] = old
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
	if len(q.CQ) == 0 {
		return nil, nil
	}

	var list = make([]ContinuousQuery, 0, len(q.CQ))
	for _, cq := range q.CQ {
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
	if q.CQ == nil {
		return ContinuousQuery{}, ErrRecordNotFound
	}
	cq, ok := q.CQ[id]
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
	if q.CQ == nil {
		q.CQ = map[string]ContinuousQuery{}
	}

	id := GenerateID()
	q.CQ[id] = cq
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
	if q.CQ == nil {
		return nil
	}
	delete(q.CQ, id)
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
	if q.CQ == nil {
		return ErrRecordNotFound
	}

	if _, ok := q.CQ[id]; !ok {
		return ErrRecordNotFound
	}
	q.CQ[id] = cq
	h.queries[query] = q
	return h.save()
}
