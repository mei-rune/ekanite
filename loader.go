package ekanite

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type LazyIndex struct {
	loader    *IndexLoader
	id        int
	path      string    // Path to shard data
	startTime time.Time // Start-time inclusive for this index
	endTime   time.Time // End-time exclusive for this index
}

func (i *LazyIndex) find() *Index {
	return i.loader.find(i)
}

func (i *LazyIndex) Load() (*Index, error) {
	return i.loader.load(i)
}

func (i *LazyIndex) Close() error {
	return i.loader.unload(i)
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
	idSeed        int
	mu            sync.RWMutex
	allIndexes    LazyIndexes
	fixIndexes    Indexes
	latestIndexes Indexes
}

// Open opens the engine.
func (il *IndexLoader) Open(pa string) error {
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
	return nil
}

func (loader *IndexLoader) Close() error {
	for _, idx := range loader.fixIndexes {
		if err := idx.Close(); err != nil {
			return err
		}
	}
	loader.fixIndexes = nil
	for _, idx := range loader.latestIndexes {
		if err := idx.Close(); err != nil {
			return err
		}
	}
	loader.latestIndexes = nil
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

func (loader *IndexLoader) find(li *LazyIndex) *Index {
	for _, idx := range loader.fixIndexes {
		if idx.id == li.id {
			return idx
		}
	}
	for _, idx := range loader.latestIndexes {
		if idx.id == li.id {
			return idx
		}
	}
	return nil
}

func (loader *IndexLoader) load(li *LazyIndex) (*Index, error) {
	idx := loader.find(li)
	if idx != nil {
		return idx, nil
	}
	idx, err := OpenIndex(li.id, li.path)
	if err != nil {
		return nil, err
	}

	loader.latestIndexes = append(loader.latestIndexes, idx)
	return idx, nil
}

func (loader *IndexLoader) unload(li *LazyIndex) error {
	for pos, idx := range loader.fixIndexes {
		if idx.id == li.id {
			newLen := len(loader.fixIndexes) - 1
			if pos != newLen {
				copy(loader.fixIndexes[pos:], loader.fixIndexes[pos+1:])
			}
			loader.fixIndexes = loader.fixIndexes[:newLen]
			return idx.Close()
		}
	}
	for pos, idx := range loader.latestIndexes {
		if idx.id == li.id {
			newLen := len(loader.latestIndexes) - 1
			if pos != newLen {
				copy(loader.latestIndexes[pos:], loader.latestIndexes[pos+1:])
			}
			loader.latestIndexes = loader.latestIndexes[:newLen]
			return idx.Close()
		}
	}
	return nil
}

func (im *IndexLoader) newIndex(pa string, startTime, endTime time.Time, numShards int) *LazyIndex {
	im.idSeed++
	i := &LazyIndex{
		loader:    im,
		id:        im.idSeed,
		path:      pa,
		startTime: startTime,
		endTime:   endTime,
	}
	im.allIndexes = append(im.allIndexes, i)
	sort.Sort(im.allIndexes)
	return i
}

func (loader *IndexLoader) GetIndexes(startTime, endTime time.Time) []*LazyIndex {
	loader.mu.RUnlock()
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
