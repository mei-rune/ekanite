package ekanite

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	bleve_index "github.com/blevesearch/bleve/index"
)

// Engine defaults
const (
	DefaultNumShards       = 4
	DefaultIndexDuration   = 24 * time.Hour
	DefaultRetentionPeriod = 7 * 24 * time.Hour

	RetentionCheckInterval = time.Hour
)

// Engine stats
var (
	stats = expvar.NewMap("engine")
)

// Searcher is the interface any object that perform searches should implement.
type Searcher interface {
	Query(ctx context.Context, startTime, endTime time.Time, req *bleve.SearchRequest,
		cb func(*bleve.SearchRequest, *bleve.SearchResult) error) error
	Fields(ctx context.Context, startTime, endTime time.Time) ([]string, error)
	FieldDict(ctx context.Context, startTime, endTime time.Time, field string) ([]bleve_index.DictEntry, error)
}

// EventIndexer is the interface a system than can index events must implement.
type EventIndexer interface {
	Index(events []Document) error
}

// Batcher accepts "input events", and once it has a certain number, or a certain amount
// of time has passed, sends those as indexable Events to an Indexer. It also supports a
// maximum number of unprocessed Events it will keep pending. Once this limit is reached,
// it will not accept anymore until outstanding Events are processed.
type Batcher struct {
	indexer  EventIndexer
	size     int
	duration time.Duration

	c chan Document
}

// NewBatcher returns a Batcher for EventIndexer e, a batching size of sz, a maximum duration
// of dur, and a maximum outstanding count of max.
func NewBatcher(e EventIndexer, sz int, dur time.Duration, max int) *Batcher {
	return &Batcher{
		indexer:  e,
		size:     sz,
		duration: dur,
		c:        make(chan Document, max),
	}
}

// Start starts the batching process.
func (b *Batcher) Start(errChan chan<- error) error {
	go func() {
		batch := make([]Document, 0, b.size)
		timer := time.NewTimer(b.duration)
		timer.Stop() // Stop any first firing.

		send := func() {
			err := b.indexer.Index(batch)
			if err != nil {
				stats.Add("batchIndexedError", 1)
				return
			}
			stats.Add("batchIndexed", 1)
			stats.Add("eventsIndexed", int64(len(batch)))
			if errChan != nil {
				errChan <- err
			}
			batch = make([]Document, 0, b.size)
		}

		for {
			select {
			case event := <-b.c:
				batch = append(batch, event)
				if len(batch) == 1 {
					timer.Reset(b.duration)
				}
				if len(batch) == b.size {
					timer.Stop()
					send()
				}
			case <-timer.C:
				stats.Add("batchTimeout", 1)
				send()
			}
		}
	}()

	return nil
}

// Stop stops the batching process.
func (b *Batcher) Stop() {
	close(b.c)
}

// C returns the channel on the batcher to which events should be sent.
func (b *Batcher) C() chan<- Document {
	return b.c
}

// Engine is the component that performs all indexing.
type Engine struct {
	path            string        // Path to all indexed data
	NumShards       int           // Number of shards to use when creating an index.
	IndexDuration   time.Duration // Duration of created indexes.
	NumCaches       int           // Number of caches to use when search in index.
	RetentionPeriod time.Duration // How long after Index end-time to hang onto data.

	mu      sync.RWMutex
	indexes Indexes

	open bool
	done chan struct{}
	wg   sync.WaitGroup

	Logger *log.Logger
}

// NewEngine returns a new indexing engine, which will use any data located at path.
func NewEngine(path string) *Engine {
	return &Engine{
		path:            path,
		NumShards:       DefaultNumShards,
		IndexDuration:   DefaultIndexDuration,
		RetentionPeriod: DefaultRetentionPeriod,
		done:            make(chan struct{}),
		Logger:          log.New(os.Stderr, "[engine] ", log.LstdFlags),
	}
}

// Open opens the engine.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.path, 0755); err != nil {
		return err
	}
	d, err := os.Open(e.path)
	if err != nil {
		return fmt.Errorf("failed to open engine: %s", err.Error())
	}

	fis, err := d.Readdir(0)
	if err != nil {
		return err
	}
	d.Close()

	// Open all indexes.
	for _, fi := range fis {
		if !fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		indexPath := filepath.Join(e.path, fi.Name())
		i, err := OpenIndex(indexPath)
		if err != nil {
			return fmt.Errorf("engine failed to open at index %s: %s", indexPath, err.Error())
		}
		log.Printf("engine opened index with %d shard(s) at %s", len(i.Shards), indexPath)
		e.indexes = append(e.indexes, i)
		sort.Sort(e.indexes)
	}

	e.wg.Add(1)
	go e.runRetentionEnforcement()

	e.open = true
	return nil
}

// Close closes the engine.
func (e *Engine) Close() error {
	if !e.open {
		return nil
	}

	for _, i := range e.indexes {
		if err := i.Close(); err != nil {
			return err
		}
	}

	close(e.done)
	e.wg.Wait()

	e.open = false
	return nil
}

// Total returns the total number of documents indexed.
func (e *Engine) Total() (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total uint64
	for _, i := range e.indexes {
		t, err := i.Total()
		if err != nil {
			return 0, err
		}
		total += t
	}
	return total, nil
}

// runRetentionEnforcement periodically runs retention enforcement.
func (e *Engine) runRetentionEnforcement() {
	defer e.wg.Done()
	for {
		select {
		case <-e.done:
			return

		case <-time.After(RetentionCheckInterval):
			stats.Add("retentionEnforcementRun", 1)
			e.enforceRetention()
		}
	}
}

// enforceRetention removes indexes which have aged out.
func (e *Engine) enforceRetention() {
	e.mu.Lock()
	defer e.mu.Unlock()

	filtered := e.indexes[:0]
	for _, i := range e.indexes {
		if i.Expired(time.Now().UTC(), e.RetentionPeriod) {
			if err := DeleteIndex(i); err != nil {
				e.Logger.Printf("retention enforcement failed to delete index %s: %s", i.path, err.Error())
			} else {
				e.Logger.Printf("retention enforcement deleted index %s", i.path)
				stats.Add("retentionEnforcementDeletions", 1)
			}
		} else {
			filtered = append(filtered, i)
		}
	}
	e.indexes = filtered
	return
}

// indexForReferenceTime returns an index suitable for indexing an event
// for the given reference time. Must be called under RLock.
func (e *Engine) indexForReferenceTime(t time.Time) *Index {
	for _, i := range e.indexes {
		if i.Contains(t) {
			return i
		}
	}
	return nil
}

// getIndexs get all index with a given start and end time and it must be called under lock.
func (e *Engine) getIndexs(startTime, endTime time.Time) []*Index {
	if startTime.IsZero() {
		if endTime.IsZero() {
			return e.indexes
		}

		var indexes []*Index
		for _, idx := range e.indexes {
			if endTime.Before(idx.startTime) {
				continue
			}
			indexes = append(indexes, idx)
		}
		return indexes
	} else if endTime.IsZero() {
		var indexes []*Index
		for _, idx := range e.indexes {
			if startTime.After(idx.endTime) {
				continue
			}
			indexes = append(indexes, idx)
		}
		return indexes
	}

	var indexes []*Index
	for _, idx := range e.indexes {
		if endTime.Before(idx.startTime) {
			//  实际数据 -------s-----e---
			//  情况  1  --s--e-----------
			continue
		}

		if startTime.After(idx.endTime) {
			//  实际数据 --s--e-------
			//  情况  1  -------s--e--
			continue
		}

		indexes = append(indexes, idx)
	}

	return indexes
}

// createIndex creates an index with a given start and end time and adds the
// created index to the Engine's store. It must be called under lock.
func (e *Engine) createIndex(startTime, endTime time.Time) (*Index, error) {
	// There cannot be two indexes with the same start time, since this would mean
	// two indexes with the same path. So if an index already exists with the requested
	// start time, use that index's end time as the start time.
	var idx *Index
	for _, i := range e.indexes {
		if i.startTime == startTime {
			idx = i
			break
		}
	}
	if idx != nil {
		startTime = idx.endTime // XXX This could still align with another start time! Needs some sort of loop.
		assert(!startTime.After(endTime), "new start time after end time")
	}

	i, err := NewIndex(e.path, startTime, endTime, e.NumShards)
	if err != nil {
		return nil, err
	}
	e.indexes = append(e.indexes, i)
	sort.Sort(e.indexes)

	e.Logger.Printf("index %s created with %d shards, start time: %s, end time: %s",
		i.Path(), e.NumShards, i.StartTime(), i.EndTime())
	return i, nil
}

// createIndexForReferenceTime creates an index suitable for indexing an event at the given
// reference time.
func (e *Engine) createIndexForReferenceTime(rt time.Time) (*Index, error) {
	start := rt.Truncate(e.IndexDuration).UTC()
	end := start.Add(e.IndexDuration).UTC()
	return e.createIndex(start, end)
}

// Index indexes a batch of Events. It blocks until all processing has completed.
func (e *Engine) Index(events []Document) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var wg sync.WaitGroup

	// De-multiplex the batch into sub-batches, one sub-batch for each Index.
	subBatches := make(map[*Index][]Document, 0)

	for _, ev := range events {
		index := e.indexForReferenceTime(ev.ReferenceTime())
		if index == nil {
			func() {
				// Take a RWLock, check again, and create a new index if necessary.
				// Doing this in a function makes lock management foolproof.
				e.mu.RUnlock()
				defer e.mu.RLock()
				e.mu.Lock()
				defer e.mu.Unlock()

				index = e.indexForReferenceTime(ev.ReferenceTime())
				if index == nil {
					var err error
					index, err = e.createIndexForReferenceTime(ev.ReferenceTime())
					if err != nil || index == nil {
						panic(fmt.Sprintf("failed to create index for %s: %s", ev.ReferenceTime(), err))
					}
				}
			}()
		}

		if _, ok := subBatches[index]; !ok {
			subBatches[index] = make([]Document, 0)
		}
		subBatches[index] = append(subBatches[index], ev)
	}

	var mu sync.Mutex
	var errList []error
	// Index each batch in parallel.
	for index, subBatch := range subBatches {
		wg.Add(1)
		go func(i *Index, b []Document) {
			defer wg.Done()
			if err := i.Index(b); err != nil {
				mu.Lock()
				errList = append(errList, err)
				mu.Unlock()
			}
		}(index, subBatch)
	}
	wg.Wait()

	if len(errList) != 0 {
		return ErrArray(errList)
	}
	return nil
}

func (e *Engine) Query(ctx context.Context, startTime, endTime time.Time, req *bleve.SearchRequest, cb func(*bleve.SearchRequest, *bleve.SearchResult) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats.Add("queriesRx", 1)

	indexes := e.getIndexs(startTime, endTime)
	if len(indexes) == 0 {
		return bleve.ErrorAliasEmpty
	}

	var indexAlias = make([]bleve.Index, 0, len(indexes)*e.NumShards)
	for _, idx := range indexes {
		for _, shard := range idx.Shards {
			indexAlias = append(indexAlias, shard.b)
		}
	}

	result, err := bleve.MultiSearch(ctx, req, indexAlias...)
	if err != nil {
		return err
	}
	return cb(req, result)
}

func (e *Engine) Fields(ctx context.Context, startTime, endTime time.Time) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats.Add("queriesRx", 1)

	indexes := e.getIndexs(startTime, endTime)
	if len(indexes) == 0 {
		return nil, bleve.ErrorAliasEmpty
	}

	var indexAlias = 0
	for _, idx := range indexes {
		indexAlias += len(idx.Shards)
	}

	var wait sync.WaitGroup
	c := make(chan struct {
		err    error
		fields []string
	}, indexAlias)

	wait.Add(indexAlias)

	for _, idx := range indexes {
		for _, shard := range idx.Shards {
			go func(shard *Shard) {
				defer wait.Done()

				fields, err := shard.b.Fields()
				c <- struct {
					err    error
					fields []string
				}{err: err, fields: fields}
			}(shard)
		}
	}

	wait.Wait()
	close(c)

	var allFields = map[string]struct{}{}
	var errList []error
	for r := range c {
		for _, field := range r.fields {
			allFields[field] = struct{}{}
		}
		if r.err != nil {
			errList = append(errList, r.err)
		}
	}
	if len(errList) > 0 {
		var buf bytes.Buffer
		for _, err := range errList {
			buf.WriteString(err.Error())
			buf.WriteString("\n")
		}
		return nil, errors.New(buf.String())
	}

	fields := make([]string, 0, len(allFields))
	for k := range allFields {
		fields = append(fields, k)
	}
	return fields, nil
}

func (e *Engine) FieldDict(ctx context.Context, startTime, endTime time.Time, field string) ([]bleve_index.DictEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats.Add("queriesRx", 1)

	indexes := e.getIndexs(startTime, endTime)
	if len(indexes) == 0 {
		return nil, bleve.ErrorAliasEmpty
	}

	var indexAlias = 0
	for _, idx := range indexes {
		indexAlias += len(idx.Shards)
	}

	var wait sync.WaitGroup
	c := make(chan struct {
		err     error
		entries []bleve_index.DictEntry
	}, indexAlias)

	wait.Add(indexAlias)

	for _, idx := range indexes {
		for _, shard := range idx.Shards {
			go func(shard *Shard) {
				defer wait.Done()

				dict, err := shard.b.FieldDict(field)
				if err != nil {
					c <- struct {
						err     error
						entries []bleve_index.DictEntry
					}{err: err}
					return
				}
				defer dict.Close()

				var entries []bleve_index.DictEntry
				for {
					entry, err := dict.Next()
					if err != nil {
						c <- struct {
							err     error
							entries []bleve_index.DictEntry
						}{err: err}
						return
					}
					if entry == nil {
						break
					}
					entries = append(entries, *entry)
				}

				c <- struct {
					err     error
					entries []bleve_index.DictEntry
				}{entries: entries}
			}(shard)
		}
	}

	wait.Wait()
	close(c)

	var allEntries = map[string]*bleve_index.DictEntry{}
	var errList []error
	for r := range c {
		for _, entry := range r.entries {
			if old := allEntries[entry.Term]; old == nil {
				copyed := new(bleve_index.DictEntry)
				*copyed = entry
				allEntries[entry.Term] = copyed
			} else {
				old.Count += entry.Count
			}
		}

		if r.err != nil {
			errList = append(errList, r.err)
		}
	}
	if len(errList) > 0 {
		var buf bytes.Buffer
		for _, err := range errList {
			buf.WriteString(err.Error())
			buf.WriteString("\n")
		}
		return nil, errors.New(buf.String())
	}

	entries := make([]bleve_index.DictEntry, 0, len(allEntries))
	for _, v := range allEntries {
		entries = append(entries, *v)
	}
	return entries, nil
}

// Search performs a search.
func (e *Engine) Search(ctx context.Context, query string) (<-chan string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats.Add("queriesRx", 1)

	// Buffer channel to control how many docs are sent back.
	c := make(chan string, 1)

	go func() {
		// Sequentially search each index, starting with the earliest in time.
		// This could be done in parallel but more sorting would be required.
		for i := len(e.indexes) - 1; i >= 0; i-- {
			e.Logger.Printf("searching index %s", e.indexes[i].Path())
			ids, err := e.indexes[i].Search(query)
			if err != nil {
				e.Logger.Println("error performing search:", err.Error())
				break
			}
			for _, id := range ids {
				b, err := e.indexes[i].Document(id)
				if err != nil {
					e.Logger.Println("error getting document:", err.Error())
					break
				}
				stats.Add("docsIDsRetrived", 1)
				c <- string(b) // There is excessive byte-slice-to-strings here.
			}
		}
		close(c)
	}()

	return c, nil
}

// Path returns the path to the directory of indexed data.
func (e *Engine) Path() string {
	return e.path
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

func SearchString(ctx context.Context, logger *log.Logger, searcher Searcher, q string) (<-chan string, error) {
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	searchRequest.Size = MaxSearchHitSize

	// validate the query
	err := query.Validate()
	if err != nil {
		return nil, err
	}

	// Buffer channel to control how many docs are sent back.
	c := make(chan string, 1)
	go func() {
		defer close(c)

		// execute the query
		err := searcher.Query(ctx, time.Time{}, time.Now(), searchRequest, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
			for _, doc := range resp.Hits {
				// bs, err := doc.Index.GetInternal([]byte(doc.Doc.ID))
				// if err != nil {
				// 	return err
				// }
				c <- fmt.Sprint(doc.Fields["message"])
			}
			return nil
		})
		if err != nil {
			logger.Println("error getting document:", err.Error())
		}
	}()

	return c, nil
}
