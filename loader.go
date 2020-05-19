package ekanite

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LazyIndex struct {
	loader    *IndexLoader
	isNew     bool
	id        int
	path      string    // Path to shard data
	startTime time.Time // Start-time inclusive for this index
	endTime   time.Time // End-time exclusive for this index
}

func (i *LazyIndex) Load(ctx context.Context) (*ResourceIndex, error) {
	return i.loader.Load(ctx, i)
}

// Path returns the path to storage for the index.
func (i *LazyIndex) Path() string { return i.path }

// StartTime returns the inclusive start time of the index.
func (i *LazyIndex) StartTime() time.Time { return i.startTime }

// EndTime returns the exclusive end time of the index.
func (i *LazyIndex) EndTime() time.Time { return i.endTime }

// Expired returns whether the index has expired at the given time, if the
// retention period is r.
func (i *LazyIndex) Expired(t time.Time, r time.Duration) bool {
	return i.endTime.Add(r).Before(t)
}

// Contains returns whether the index's time range includes the given
// reference time.
func (i *LazyIndex) Contains(t time.Time) bool {
	return (t.Equal(i.startTime) || t.After(i.startTime)) && t.Before(i.endTime)
}

// Indexes is a slice of indexes.
type LazyIndexes []*LazyIndex

// Indexes are ordered by decreasing end time.
// If two indexes have the same end time, then order by decreasing start time.
// This means that the first index in the slice covers the latest time range.
func (i LazyIndexes) Len() int { return len(i) }
func (i LazyIndexes) Less(u, v int) bool {
	if i[u].endTime.After(i[v].endTime) {
		return true
	}
	return i[u].startTime.After(i[v].startTime)
}
func (i LazyIndexes) Swap(u, v int) { i[u], i[v] = i[v], i[u] }

// OpenIndex opens an existing index, at the given path.
func OpenLazyIndex(path string) (*LazyIndex, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to access index at %s", path)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("index %s path is not a directory", path)
	}

	// Get the start time and end time.
	startTime, err := time.Parse(indexNameLayout, fi.Name())
	if err != nil {
		return nil, fmt.Errorf("unable to determine start time of index: %s", err.Error())
	}

	var endTime time.Time
	f, err := os.Open(filepath.Join(path, endTimeFileName))
	if err != nil {
		return nil, fmt.Errorf("unable to open end time file for index: %s", err.Error())
	}
	defer f.Close()
	r := bufio.NewReader(f)
	s, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("unable to determine end time of index: %s", err.Error())
	}
	endTime, err = time.Parse(indexNameLayout, s)
	if err != nil {
		return nil, fmt.Errorf("unable to parse end time from '%s': %s", s, err.Error())
	}

	// Index is ready to go.
	return &LazyIndex{
		path:      path,
		startTime: startTime,
		endTime:   endTime,
	}, nil
}

type IndexLoader struct {
	isClosed   int32
	idSeed     int
	path       string
	numCaches  int
	numShards  int
	mu         sync.RWMutex
	allIndexes LazyIndexes
	// fixIndexes    Indexes
	latestIndexes resourceSemaphore
}

// Open opens the engine.
func (il *IndexLoader) Open(pa string, numShards, numCaches int) error {
	if err := os.MkdirAll(pa, 0755); err != nil {
		return err
	}
	d, err := os.Open(pa)
	if err != nil {
		return fmt.Errorf("failed to open engine: %s", err.Error())
	}

	fis, err := d.Readdir(0)
	if err != nil {
		return err
	}
	d.Close()

	// Open all loaders.
	for _, fi := range fis {
		if !fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		indexPath := filepath.Join(pa, fi.Name())
		i, err := OpenLazyIndex(indexPath)
		if err != nil {
			return fmt.Errorf("engine failed to open at index %s: %s", indexPath, err.Error())
		}
		il.idSeed++
		i.loader = il
		i.id = il.idSeed
		log.Printf("engine opened index at %s", indexPath)
		il.allIndexes = append(il.allIndexes, i)
		sort.Sort(il.allIndexes)
	}

	il.path = pa
	il.numShards = numShards
	il.numCaches = numCaches
	if il.numCaches == 0 {
		il.numCaches = 2
	}
	il.latestIndexes.init(il.numCaches)
	return nil
}

func (loader *IndexLoader) Close() error {
	if !atomic.CompareAndSwapInt32(&loader.isClosed, 0, 1) {
		return nil
	}

	// for _, idx := range loader.fixIndexes {
	// 	if err := idx.Close(); err != nil {
	// 		return err
	// 	}
	// }
	// loader.fixIndexes = nil

	if err := loader.latestIndexes.Close(); err != nil {
		return err
	}
	return nil
}

// indexForReferenceTime returns an index suitable for indexing an event
// for the given reference time. Must be called under RLock.
func (loader *IndexLoader) indexForReferenceTime(t time.Time) *LazyIndex {
	for _, idx := range loader.allIndexes {
		if idx.Contains(t) {
			return idx
		}
	}
	return nil
}

// getIndexs get all index with a given start and end time and it must be called under lock.
func (loader *IndexLoader) getIndexs(startTime, endTime time.Time) []*LazyIndex {
	if startTime.IsZero() {
		if endTime.IsZero() {
			return loader.allIndexes
		}

		var indexes []*LazyIndex
		for _, idx := range loader.allIndexes {
			if endTime.Before(idx.startTime) {
				continue
			}
			indexes = append(indexes, idx)
		}
		return indexes
	} else if endTime.IsZero() {
		var indexes []*LazyIndex
		for _, idx := range loader.allIndexes {
			if startTime.After(idx.endTime) {
				continue
			}
			indexes = append(indexes, idx)
		}
		return indexes
	}

	var indexes []*LazyIndex
	for _, idx := range loader.allIndexes {
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

func (loader *IndexLoader) newIndex(startTime, endTime time.Time) *LazyIndex {
	loader.idSeed++
	i := &LazyIndex{
		loader:    loader,
		isNew:     true,
		id:        loader.idSeed,
		path:      loader.path,
		startTime: startTime,
		endTime:   endTime,
	}
	loader.allIndexes = append(loader.allIndexes, i)
	sort.Sort(loader.allIndexes)
	return i
}

func (loader *IndexLoader) GetIndexes(startTime, endTime time.Time) []*LazyIndex {
	loader.mu.RLock()
	defer loader.mu.RUnlock()

	return loader.getIndexs(startTime, endTime)
}

func (loader *IndexLoader) Do(cb func(loader *IndexLoader, switchFunc func())) {
	isReadonly := true

	loader.mu.RLock()
	defer func() {
		if isReadonly {
			loader.mu.RUnlock()
		} else {
			loader.mu.Unlock()
		}
	}()

	cb(loader, func() {
		if isReadonly {
			loader.mu.RUnlock()
			loader.mu.Lock()
			isReadonly = false
		}
	})
}

type ResourceIndex struct {
	*Index
	r      *resource
	loader *IndexLoader
}

func (ri *ResourceIndex) Close() error {
	ri.loader.latestIndexes.Release(ri.r)
	return nil
}

func (loader *IndexLoader) Load(ctx context.Context, li *LazyIndex) (*ResourceIndex, error) {
	r, err := loader.latestIndexes.TryAcquire(ctx, li.id, false)
	if err != nil {
		return nil, errors.New("load '" + li.path + "':" + err.Error())
	}

	loader.mu.RLock()
	isNew := li.isNew
	pa := li.path
	loader.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.index != nil {
		if r.id == r.index.id {
			return &ResourceIndex{r.index, r, loader}, nil
		}
		if err := r.index.Close(); err != nil {
			loader.latestIndexes.Release(r)
			return nil, err
		}
		r.index = nil
	}

	if isNew {
		idx, err := NewIndex(li.id, pa, li.startTime, li.endTime, loader.numShards)
		if err != nil {
			loader.latestIndexes.Release(r)
			return nil, err
		}
		go func() {
			loader.mu.Lock()
			li.isNew = false
			li.path = idx.path
			loader.mu.Unlock()
		}()

		r.index = idx
		return &ResourceIndex{idx, r, loader}, nil

	}
	idx, err := OpenIndex(li.id, pa, li.startTime, li.endTime)
	if err != nil {
		loader.latestIndexes.Release(r)
		return nil, err
	}
	r.index = idx
	return &ResourceIndex{idx, r, loader}, nil
}

// func (loader *IndexLoader) unload(li *LazyIndex) error {
// 	for pos, idx := range loader.fixIndexes {
// 		if idx.id == li.id {
// 			newLen := len(loader.fixIndexes) - 1
// 			if pos != newLen {
// 				copy(loader.fixIndexes[pos:], loader.fixIndexes[pos+1:])
// 			}
// 			loader.fixIndexes = loader.fixIndexes[:newLen]
// 			return idx.Close()
// 		}
// 	}
// 	for pos, idx := range loader.latestIndexes {
// 		if idx.id == li.id {
// 			newLen := len(loader.latestIndexes) - 1
// 			if pos != newLen {
// 				copy(loader.latestIndexes[pos:], loader.latestIndexes[pos+1:])
// 			}
// 			loader.latestIndexes = loader.latestIndexes[:newLen]
// 			return idx.Close()
// 		}
// 	}
// 	return nil
// }
