package ekanite

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"os"
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

type Continuation struct {
	lastest *ResourceIndex
}

func (c *Continuation) Close() error {
	if c.lastest == nil {
		return nil
	}
	err := c.lastest.Close()
	c.lastest = nil
	return err
}

// EventIndexer is the interface a system than can index events must implement.
type EventIndexer interface {
	Index(ctx *Continuation, events []Document) error
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
		var ctx Continuation
		batch := make([]Document, 0, b.size)
		timer := time.NewTimer(b.duration)
		timer.Stop() // Stop any first firing.

		defer CloseWith(&ctx)

		send := func() {
			err := b.indexer.Index(&ctx, batch)
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
	NumCaches       int           // Number of caches to use when search in index.
	IndexDuration   time.Duration // Duration of created indexes.
	RetentionPeriod time.Duration // How long after Index end-time to hang onto data.

	indexes IndexLoader

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
	if err := e.indexes.Open(e.path, e.NumShards, e.NumCaches, e.IndexDuration); err != nil {
		return err
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

	if err := e.indexes.Close(); err != nil {
		return err
	}

	close(e.done)
	e.wg.Wait()

	e.open = false
	return nil
}

// Total returns the total number of documents indexed.
func (e *Engine) Total() (uint64, error) {
	var total uint64
	var errList []error

	e.indexes.Do(func(loader *IndexLoader, switchFunc func()) {
		for _, li := range e.indexes.allIndexes {
			t, err := func() (uint64, error) {
				i, err := li.Load(context.Background())
				if err != nil {
					return 0, err
				}
				defer CloseWith(i)

				return i.Total()
			}()
			if err != nil {
				errList = append(errList, err)
				return
			}
			total += t
		}
	})

	if len(errList) != 0 {
		return 0, ErrArray(errList)
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
	e.indexes.Do(func(loader *IndexLoader, switchFunc func()) {
		filtered := loader.allIndexes[:0]
		for _, i := range loader.allIndexes {
			if i.Expired(time.Now().UTC(), e.RetentionPeriod) {
				// if err := i.Close(); err != nil {
				// 	e.Logger.Printf("retention enforcement failed to close index %s: %s", i.path, err.Error())
				// 	continue
				// }

				if err := os.RemoveAll(i.path); err != nil {
					e.Logger.Printf("retention enforcement failed to delete index %s: %s", i.path, err.Error())
				} else {
					e.Logger.Printf("retention enforcement deleted index %s", i.path)
					stats.Add("retentionEnforcementDeletions", 1)
				}
			} else {
				filtered = append(filtered, i)
			}
		}
		loader.allIndexes = filtered
	})
	return
}

// createIndex creates an index with a given start and end time and adds the
// created index to the Engine's store. It must be called under lock.
func (e *Engine) createIndex(loader *IndexLoader, startTime, endTime time.Time) *LazyIndex {
	// There cannot be two indexes with the same start time, since this would mean
	// two indexes with the same path. So if an index already exists with the requested
	// start time, use that index's end time as the start time.
	var idx *LazyIndex
	for _, i := range loader.allIndexes {
		if i.startTime == startTime {
			idx = i
			break
		}
	}
	if idx != nil {
		startTime = idx.endTime // XXX This could still align with another start time! Needs some sort of loop.
		assert(!startTime.After(endTime), "new start time after end time")
	}

	i := loader.newIndex(startTime, endTime)

	e.Logger.Printf("index %s created with %d shards, start time: %s, end time: %s",
		i.Path(), e.NumShards, i.StartTime(), i.EndTime())
	return i
}

// Index indexes a batch of Events. It blocks until all processing has completed.
func (e *Engine) Index(ctx *Continuation, events []Document) error {
	var wg sync.WaitGroup

	// De-multiplex the batch into sub-batches, one sub-batch for each Index.
	subBatches := make(map[*LazyIndex][]Document, 0)

	e.indexes.Do(func(loader *IndexLoader, switchFunc func()) {
		for _, ev := range events {
			index := loader.indexForReferenceTime(ev.ReferenceTime())
			if index == nil {
				// Take a RWLock, check again, and create a new index if necessary.
				// Doing this in a function makes lock management foolproof.
				switchFunc()

				index = loader.indexForReferenceTime(ev.ReferenceTime())
				if index == nil {
					start := ev.ReferenceTime().Truncate(e.IndexDuration).UTC()
					end := start.Add(e.IndexDuration).UTC()
					index = e.createIndex(loader, start, end)
				}
			}

			if _, ok := subBatches[index]; !ok {
				subBatches[index] = make([]Document, 0)
			}
			subBatches[index] = append(subBatches[index], ev)
		}
	})

	if len(subBatches) > 1 {
		if ctx.lastest != nil {
			CloseWith(ctx.lastest)
			ctx.lastest = nil
		}
	}

	var mu sync.Mutex
	var errList []error
	// Index each batch in parallel.
	for lazyIndex, subBatch := range subBatches {
		wg.Add(1)
		go func(li *LazyIndex, b []Document) {
			defer wg.Done()

			var i *ResourceIndex
			if ctx.lastest != nil {
				if ctx.lastest.r.id == li.id {
					i = ctx.lastest
				}
			}
			if i == nil {
				var err error
				i, err = lazyIndex.Load(context.Background())
				if err != nil {
					mu.Lock()
					errList = append(errList, err)
					mu.Unlock()
					return
				}
				if len(subBatches) == 1 {
					ctx.lastest = i
				} else {
					defer CloseWith(i)
				}
			}
			if err := i.Index.Index(b); err != nil {
				mu.Lock()
				errList = append(errList, err)
				mu.Unlock()
			}
		}(lazyIndex, subBatch)
	}
	wg.Wait()

	if len(errList) != 0 {
		return ErrArray(errList)
	}
	return nil
}

func (e *Engine) Query(ctx context.Context, startTime, endTime time.Time, req *bleve.SearchRequest, cb func(*bleve.SearchRequest, *bleve.SearchResult) error) error {
	stats.Add("queriesRx", 1)

	indexes := e.indexes.GetIndexes(startTime, endTime)
	if len(indexes) == 0 {
		return bleve.ErrorAliasEmpty
	}

	// var indexAlias = make([]bleve.Index, 0, len(indexes)*e.NumShards)
	// for _, i := range indexes {
	// 	idx, err := i.Load(ctx)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer CloseWith(idx)

	// 	for _, shard := range idx.Shards {
	// 		indexAlias = append(indexAlias, shard.b)
	// 	}
	// }

	result, err := MultiSearch(ctx, req, indexes)
	if err != nil {
		return err
	}
	return cb(req, result)
}

func (e *Engine) Fields(ctx context.Context, startTime, endTime time.Time) ([]string, error) {
	stats.Add("queriesRx", 1)

	indexes := e.indexes.GetIndexes(startTime, endTime)
	if len(indexes) == 0 {
		return nil, bleve.ErrorAliasEmpty
	}

	var wait sync.WaitGroup
	c := make(chan []struct {
		err    error
		fields []string
	}, len(indexes))

	wait.Add(len(indexes))

	for _, i := range indexes {
		go func(li *LazyIndex) {
			defer wait.Done()

			var results []struct {
				err    error
				fields []string
			}

			idx, err := li.Load(ctx)
			if err != nil {

				results = append(results, struct {
					err    error
					fields []string
				}{err: err})

				c <- results
				return
			}
			defer CloseWith(idx)

			for _, shard := range idx.Shards {
				fields, err := shard.b.Fields()
				results = append(results, struct {
					err    error
					fields []string
				}{err: err, fields: fields})
			}

			c <- results
		}(i)
	}

	wait.Wait()
	close(c)

	var allFields = map[string]struct{}{}
	var errList []error
	for rl := range c {
		for _, r := range rl {
			for _, field := range r.fields {
				allFields[field] = struct{}{}
			}
			if r.err != nil {
				errList = append(errList, r.err)
			}
		}
	}
	if len(errList) > 0 {
		return nil, ErrArray(errList)
	}

	fields := make([]string, 0, len(allFields))
	for k := range allFields {
		fields = append(fields, k)
	}
	return fields, nil
}

func (e *Engine) FieldDict(ctx context.Context, startTime, endTime time.Time, field string) ([]bleve_index.DictEntry, error) {
	stats.Add("queriesRx", 1)

	indexes := e.indexes.GetIndexes(startTime, endTime)
	if len(indexes) == 0 {
		return nil, bleve.ErrorAliasEmpty
	}
	var wait sync.WaitGroup
	c := make(chan struct {
		err     error
		entries []bleve_index.DictEntry
	}, len(indexes))

	wait.Add(len(indexes))

	for _, i := range indexes {
		go func(li *LazyIndex) {
			defer wait.Done()

			idx, err := li.Load(ctx)
			if err != nil {
				c <- struct {
					err     error
					entries []bleve_index.DictEntry
				}{err: err}
				return
			}
			defer CloseWith(idx)

			var entries []bleve_index.DictEntry
			for _, shard := range idx.Shards {
				dict, err := shard.b.FieldDict(field)
				if err != nil {
					c <- struct {
						err     error
						entries []bleve_index.DictEntry
					}{err: err}
					return
				}
				defer dict.Close()

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
			}

			c <- struct {
				err     error
				entries []bleve_index.DictEntry
			}{entries: entries}
		}(i)
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
