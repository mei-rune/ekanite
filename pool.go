package ekanite

import (
	"context"
	"sync"
)

type resource struct {
	refCounter int32
	index      *Index
}

type resourceSemaphore struct {
	resources []*resource
	mu        sync.Mutex
	cond      sync.Cond
}

func (rs *resourceSemaphore) init() {
	rs.cond.L = &rs.mu
}

func idEqual(id int) func(*resource) bool {
	return func(r *resource) bool {
		if r.index == nil {
			return false
		}
		return r.index.id == id
	}
}

func (rs *resourceSemaphore) TryAcquire(ctx context.Context, pred func(*resource) bool) *resource {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for _, r := range rs.resources {
		if pred(r) {
			r.refCounter++
			return r
		}
	}

	c := make(chan struct{}, 1)

	for {
		for _, r := range rs.resources {
			if r.refCounter == 0 {
				r.refCounter++
				return r
			}
		}

		go func() {
			rs.cond.Wait()
			c <- struct{}{}
		}()
		select {
		case <-ctx.Done():
			rs.cond.Broadcast() // 让 goroutine 可以退出
			return nil
		case <-c:
		}
	}
}

func (rs *resourceSemaphore) Release(r *resource) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	r.refCounter--
	if r.refCounter == 0 {
		rs.cond.Broadcast()
	}
}
