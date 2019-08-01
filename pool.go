package ekanite

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
)

type resource struct {
	id         int
	refCounter int32

	mu    sync.Mutex
	index *Index
}

type resourceSemaphore struct {
	isClosed  bool
	resources []*resource
	mu        sync.Mutex
	cond      sync.Cond
}

func (rs *resourceSemaphore) init(size int) {
	if size == 0 {
		panic("size is zero")
	}
	rs.cond.L = &rs.mu
	rs.resources = make([]*resource, size)
	for idx := range rs.resources {
		rs.resources[idx] = new(resource)
	}
}

func (rs *resourceSemaphore) Close() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.isClosed {
		return nil
	}
	rs.isClosed = true

	rs.cond.Broadcast()

	for _, idx := range rs.resources {
		if idx.refCounter == 0 && idx.index != nil {
			if err := idx.index.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func dumpStack() string {
	return string(debug.Stack())
}

var ErrNotFound = errors.New("not found")

func (rs *resourceSemaphore) TryAcquire(ctx context.Context, id int, donotWait bool) (*resource, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(rs.resources) == 0 {
		panic("resources is empty")
	}

	for _, r := range rs.resources {
		if r.id == id {
			r.refCounter++
			return r, nil
		}
	}

	if donotWait {
		return nil, ErrNotFound
	}

	c := make(chan struct{}, 1)

	for !rs.isClosed {
		for _, r := range rs.resources {
			if r.refCounter == 0 {
				r.id = id
				r.refCounter++
				return r, nil
			}
		}

		go func() {
			rs.cond.Wait()
			c <- struct{}{}
		}()
		select {
		case <-ctx.Done():
			rs.cond.Broadcast() // 让 goroutine 可以退出
			return nil, ctx.Err()
		case <-c:
		}
	}
	return nil, errors.New("pool is closed")
}

func (rs *resourceSemaphore) Release(r *resource) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	r.refCounter--
	if r.refCounter == 0 {
		if rs.isClosed {
			if r.index != nil {
				r.index.Close()
			}
			return
		}
		rs.cond.Broadcast()
	}
}
