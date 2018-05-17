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

	lastAt := ekanite.AlignTime(time.Now(), s.runInterval)
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
	s.metaStore.ForEach(func(id string, q service.Query) {
		if len(q.ContinuousQueries) == 0 {
			return
		}

		keys = append(keys, id)
		qList = append(qList, q)
	})

	for idx, key := range keys {
		s.runQuery(startAt, endAt, key, &qList[idx])
	}
}

func (s *Service) isTimeField(field string) bool {
	return field == "reception"
}

// runContinuousQueries gets CQs from the meta store and runs them.
func (s *Service) runQuery(startTime, endTime time.Time, id string, qu *service.Query) {
	inclusive := true
	timeQuery := bleve.NewDateRangeInclusiveQuery(startTime, endTime, &inclusive, &inclusive)
	timeQuery.SetField("reception")

	var q query.Query
	if queries, err := qu.ToQueries(); err != nil {
		s.Logger.Println("load queries of query(id="+id+") fail,", err)
		return
	} else if len(queries) == 0 {
		q = timeQuery
	} else {
		conjunction := bleve.NewConjunctionQuery(queries...)
		conjunction.AddQuery(timeQuery)
		q = conjunction
	}

	for key, cq := range qu.ContinuousQueries {

		cb, err := s.createCallBack(&cq)
		if err != nil {
			s.Logger.Println("load callbacks of cq(query="+id+", id="+key+") fail,", err)
			continue
		}

		if cq.GroupBy == "" {
			searchRequest := bleve.NewSearchRequest(q)
			searchRequest.Fields = cq.Fields
			err := s.searcher.Query(startTime, endTime, searchRequest, toHandler(&cq, cb))
			if err != nil {
				s.Logger.Println("cq(query="+id+", id="+key+") execute fail,", err)
			}
		} else {
			err := ekanite.GroupBy(s.searcher, startTime, endTime, q, cq.GroupBy, toGroupByHandler(&cq, cb))
			if err != nil {
				s.Logger.Println("cq(query="+id+", id="+key+") execute fail,", err)
			}
		}
	}
}
