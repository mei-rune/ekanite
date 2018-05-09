package continuous_querier

import (
	"log"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
	"github.com/ekanite/ekanite/service"
)

type Service struct {
	Logger      *log.Logger
	metaStore   *service.MetaStore
	searcher    ekanite.Searcher
	runInterval time.Duration

	// RunCh can be used by clients to signal service to run CQs.
	// runCh chan struct{}
}

// NewService returns a new CQ instance.
func NewService(logger *log.Logger, searcher ekanite.Searcher, metaStore *service.MetaStore,
	stop chan struct{}, runInterval time.Duration) *Service {
	return &Service{
		Logger:      logger,
		searcher:    searcher,
		metaStore:   metaStore,
		runInterval: runInterval,
		//runCh:       make(chan struct{}),
	}
}

// func (s *Service) Signal() {
// 	select {
// 	case s.runCh <- struct{}{}:
// 	default:
// 	}
// }

// runs on a go routine and periodically executes CQs.
func (s *Service) RunLoop(stop chan struct{}) {
	t := time.NewTicker(s.runInterval)
	defer t.Stop()

	lastAt := service.AlignTime(time.Now(), s.runInterval)
	s.Logger.Println("cq last is", lastAt, ", interval is", s.runInterval)
	for {
		select {
		case <-stop:
			s.Logger.Println("continuous query service terminating")
			return
		// case _, ok := <-s.runCh:
		// 	if !ok {
		// 		s.Logger.Println("continuous query service terminating")
		// 		return
		// 	}
		// 	startAt := lastAt
		// 	lastAt = time.Now()

		// 	s.runContinuousQueries(startAt, lastAt)
		case now := <-t.C:
			endAt := lastAt.Add(s.runInterval)
			for endAt.Before(now) {
				s.Logger.Println("run cq for", lastAt, "to", endAt)
				s.runContinuousQueries(lastAt, endAt)
				lastAt = endAt
				endAt = lastAt.Add(s.runInterval)
			}
		}
	}
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQueries(startAt, endAt time.Time) {
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

		s.runContinuousQuery(startAt, endAt, key, &qList[idx], cq)
	}
}

func (s *Service) isTimeField(field string) bool {
	return field == "reception"
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runContinuousQuery(startTime, endTime time.Time, id string, qu *service.Query, cq *service.ContinuousQuery) {
	inclusive := true
	timeQuery := bleve.NewDateRangeInclusiveQuery(startTime, endTime, &inclusive, &inclusive)
	timeQuery.SetField("reception")

	var q query.Query
	if queries := qu.ToQueries(); len(queries) == 0 {
		q = timeQuery
	} else {
		conjunction := bleve.NewConjunctionQuery(queries...)
		conjunction.AddQuery(timeQuery)
		q = conjunction
	}

	cb, err := s.createCallBack(cq)
	if err != nil {
		s.Logger.Println("cq(id="+id+") fail,", err)
		return
	}

	if cq.GroupBy == "" {
		searchRequest := bleve.NewSearchRequest(q)
		searchRequest.Fields = cq.Fields
		err := s.searcher.Query(startTime, endTime, searchRequest, toHandler(cq, cb))
		if err != nil {
			s.Logger.Println("cq(id="+id+") execute fail,", err)
		}
	} else {
		err := service.GroupBy(s.searcher, startTime, endTime, q, cq.GroupBy, toGroupByHandler(cq, cb))
		if err != nil {
			s.Logger.Println("cq(id="+id+") execute fail,", err)
		}
	}
}
