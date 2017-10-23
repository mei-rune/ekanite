package continuous_querier

import (
	"time"
)

type Service struct {
	dataPath    string
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

}
