package ekanite

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
)

type asyncSearchResult struct {
	Index  bleve.Index
	Result *bleve.SearchResult
	Err    error
}

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
		Sort:             req.Sort,
		IncludeLocations: req.IncludeLocations,
	}
	return &rv
}

// MultiSearch executes a SearchRequest across multiple Index objects,
// then merges the results.  The indexes must honor any ctx deadline.
func MultiSearch(ctx context.Context, req *bleve.SearchRequest, indexes ...bleve.Index) (*SearchResult, error) {
	searchStart := time.Now()
	asyncResults := make(chan *asyncSearchResult, len(indexes))

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(in bleve.Index, childReq *bleve.SearchRequest) {
		rv := asyncSearchResult{Index: in}
		rv.Result, rv.Err = in.SearchInContext(ctx, childReq)
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

	var sr *SearchResult
	indexErrors := make(map[string]error)

	for asr := range asyncResults {
		if asr.Err == nil {
			if sr == nil {
				// first result
				sr = &SearchResult{SearchResult: asr.Result}
			} else {
				// merge with previous
				sr.Merge(asr.Result)
			}

			for _, hit := range asr.Result.Hits {
				sr.DocumentHits = append(sr.DocumentHits, &DocumentMatch{
					Index: asr.Index,
					Doc:   hit,
				})
			}
		} else {
			indexErrors[asr.Index.Name()] = asr.Err
		}
	}

	// merge just concatenated all the hits
	// now lets clean it up

	// handle case where no results were successful
	if sr == nil {
		sr = &SearchResult{SearchResult: &bleve.SearchResult{
			Status: &bleve.SearchStatus{
				Errors: make(map[string]error),
			},
		}}
	}

	// sort all hits with the requested order
	if len(req.Sort) > 0 {
		sorter := newMultiSearchHitSorter(req.Sort, sr.DocumentHits, sr.Hits)
		sort.Sort(sorter)
	}

	// now skip over the correct From
	if req.From > 0 && len(sr.Hits) > req.From {
		sr.Hits = sr.Hits[req.From:]
		sr.DocumentHits = sr.DocumentHits[req.From:]
	} else if req.From > 0 {
		sr.Hits = search.DocumentMatchCollection{}
		sr.DocumentHits = DocumentMatchCollection{}
	}

	// now trim to the correct size
	if req.Size > 0 && len(sr.Hits) > req.Size {
		sr.Hits = sr.Hits[0:req.Size]
		sr.DocumentHits = sr.DocumentHits[0:req.Size]
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
	documentHits  DocumentMatchCollection
	hits          search.DocumentMatchCollection
	sort          search.SortOrder
	cachedScoring []bool
	cachedDesc    []bool
}

func newMultiSearchHitSorter(sort search.SortOrder, docHits DocumentMatchCollection, hits search.DocumentMatchCollection) *multiSearchHitSorter {
	return &multiSearchHitSorter{
		sort:          sort,
		documentHits:  docHits,
		hits:          hits,
		cachedScoring: sort.CacheIsScore(),
		cachedDesc:    sort.CacheDescending(),
	}
}

func (m *multiSearchHitSorter) Len() int { return len(m.hits) }
func (m *multiSearchHitSorter) Swap(i, j int) {
	m.hits[i], m.hits[j] = m.hits[j], m.hits[i]
	m.documentHits[i], m.documentHits[j] = m.documentHits[j], m.documentHits[i]
}
func (m *multiSearchHitSorter) Less(i, j int) bool {
	c := m.sort.Compare(m.cachedScoring, m.cachedDesc, m.hits[i], m.hits[j])
	return c < 0
}

// A SearchResult describes the results of executing
// a SearchRequest.
type SearchResult struct {
	*bleve.SearchResult
	DocumentHits DocumentMatchCollection `json:"-"`
}

type DocumentMatch struct {
	Index bleve.Index
	Doc   *search.DocumentMatch
}

type DocumentMatchCollection []*DocumentMatch

func (c DocumentMatchCollection) Len() int           { return len(c) }
func (c DocumentMatchCollection) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c DocumentMatchCollection) Less(i, j int) bool { return c[i].Doc.Score > c[j].Doc.Score }
