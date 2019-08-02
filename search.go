package ekanite

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
)

// createChildSearchRequest creates a separate
// request from the original
// For now, avoid data race on req structure.
// TODO disable highlight/field load on child
// requests, and add code to do this only on
// the actual final results.
// Perhaps that part needs to be optional,
// could be slower in remote usages.
func createChildSearchRequest(req *bleve.SearchRequest) *bleve.SearchRequest {
	rv := bleve.SearchRequest{
		Query:            req.Query,
		Size:             req.Size + req.From,
		From:             0,
		Highlight:        req.Highlight,
		Fields:           req.Fields,
		Facets:           req.Facets,
		Explain:          req.Explain,
		Sort:             req.Sort.Copy(),
		IncludeLocations: req.IncludeLocations,
	}
	return &rv
}

type asyncSearchResult struct {
	Name   string
	Result *bleve.SearchResult
	Err    error
}

// MultiSearch executes a SearchRequest across multiple Index objects,
// then merges the results.  The indexes must honor any ctx deadline.
func MultiSearch(ctx context.Context, req *bleve.SearchRequest, indexes []*LazyIndex) (*bleve.SearchResult, error) {

	searchStart := time.Now()
	asyncResults := make(chan *asyncSearchResult, len(indexes))

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(in *LazyIndex, childReq *bleve.SearchRequest) {
		rv := asyncSearchResult{Name: in.path}
		index, err := in.Load(ctx)
		if err != nil {
			rv.Err = err
			asyncResults <- &rv
			waitGroup.Done()
			return
		}
		defer CloseWith(index)

		bs, _ := json.Marshal(childReq)
		fmt.Println(string(bs))

		rv.Result, rv.Err = index.Index.Alias.SearchInContext(ctx, childReq)
		asyncResults <- &rv
		waitGroup.Done()
	}

	waitGroup.Add(len(indexes))
	for _, in := range indexes {
		go searchChildIndex(in, createChildSearchRequest(req))
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(asyncResults)
	}()

	var sr *bleve.SearchResult
	indexErrors := make(map[string]error)

	for asr := range asyncResults {
		if asr.Err == nil {
			if sr == nil {
				// first result
				sr = asr.Result
			} else {
				// merge with previous
				sr.Merge(asr.Result)
			}
		} else {
			indexErrors[asr.Name] = asr.Err
		}
	}

	// merge just concatenated all the hits
	// now lets clean it up

	// handle case where no results were successful
	if sr == nil {
		sr = &bleve.SearchResult{
			Status: &bleve.SearchStatus{
				Errors: make(map[string]error),
			},
		}
	}

	// sort all hits with the requested order
	if len(req.Sort) > 0 {
		sorter := newMultiSearchHitSorter(req.Sort, sr.Hits)
		sort.Sort(sorter)
	}

	// now skip over the correct From
	if req.From > 0 && len(sr.Hits) > req.From {
		sr.Hits = sr.Hits[req.From:]
	} else if req.From > 0 {
		sr.Hits = search.DocumentMatchCollection{}
	}

	// now trim to the correct size
	if req.Size > 0 && len(sr.Hits) > req.Size {
		sr.Hits = sr.Hits[0:req.Size]
	}

	// fix up facets
	for name, fr := range req.Facets {
		sr.Facets.Fixup(name, fr.Size)
	}

	// fix up original request
	sr.Request = req
	searchDuration := time.Since(searchStart)
	sr.Took = searchDuration

	// fix up errors
	if len(indexErrors) > 0 {
		if sr.Status.Errors == nil {
			sr.Status.Errors = make(map[string]error)
		}
		for indexName, indexErr := range indexErrors {
			sr.Status.Errors[indexName] = indexErr
			sr.Status.Total++
			sr.Status.Failed++
		}
	}

	return sr, nil
}

type multiSearchHitSorter struct {
	hits          search.DocumentMatchCollection
	sort          search.SortOrder
	cachedScoring []bool
	cachedDesc    []bool
}

func newMultiSearchHitSorter(sort search.SortOrder, hits search.DocumentMatchCollection) *multiSearchHitSorter {
	return &multiSearchHitSorter{
		sort:          sort,
		hits:          hits,
		cachedScoring: sort.CacheIsScore(),
		cachedDesc:    sort.CacheDescending(),
	}
}

func (m *multiSearchHitSorter) Len() int      { return len(m.hits) }
func (m *multiSearchHitSorter) Swap(i, j int) { m.hits[i], m.hits[j] = m.hits[j], m.hits[i] }
func (m *multiSearchHitSorter) Less(i, j int) bool {
	c := m.sort.Compare(m.cachedScoring, m.cachedDesc, m.hits[i], m.hits[j])
	return c < 0
}
