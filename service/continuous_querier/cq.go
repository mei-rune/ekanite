package continuous_querier

import (
	"time"

	"github.com/ekanite/ekanite"
	"github.com/ekanite/ekanite/service"
)

type Service struct {
	metaStore   *service.MetaStore
	Searcher    ekanite.Searcher
	runInterval time.Duration

	// RunCh can be used by clients to signal service to run CQs.
	runCh chan struct{}
}

// backgroundLoop runs on a go routine and periodically executes CQs.
func (s *Service) backgroundLoop() {
	t := time.NewTimer(s.runInterval)
	defer t.Stop()

	for {
		select {
		case <-s.stop:
			s.Logger.Info("continuous query service terminating")
			return
		case req, ok := <-s.runCh:
			if !ok {
				s.Logger.Info("continuous query service terminating")
				return
			}
			s.runContinuousQueries()
		case <-t.c:
			s.runContinuousQueries()
		}
	}
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQueries() {
	var keys []string
	var qList []service.Query
	var cqList []service.ContinuousQuery
	s.metaStore.ForEach(func(id string, q service.QueryData) {
		if len(q.CQ) == 0 {
			return
		}

		for key, cq := range q.CQ {
			keys = append(keys, key)
			qList = append(qList, q.Query)
			cqList = append(cqList, cq)
		}
	})

	for idx, key := range keys {
		cq := &cqList[idx]

		s.runContinuousQuery(key, &qList[idx],  cq)
	}
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQuery(id string, q *service.Query, cq *service.ContinuousQuery) {
	cq.
}
