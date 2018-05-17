package continuous_querier

import (
	"errors"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/ekanite/ekanite"
	"github.com/ekanite/ekanite/service"
)

type CQHandleFunc func(cq *service.ContinuousQuery, value interface{}) error

var (
	factoryLock sync.Mutex
	factory     = map[string]func(*service.ContinuousQuery, []string) (CQHandleFunc, error){}
)

func Register(typ string, create func(*service.ContinuousQuery, []string) (CQHandleFunc, error)) {
	factoryLock.Lock()
	defer factoryLock.Unlock()
	factory[typ] = create
}

func (s *Service) createCallBack(cq *service.ContinuousQuery) (CQHandleFunc, error) {
	if cq.Callback != nil {
		return cq.Callback, nil
	}

	factoryLock.Lock()
	defer factoryLock.Unlock()

	var cbList []CQHandleFunc
	for idx := range cq.Targets {
		create, ok := factory[cq.Targets[idx].Type]
		if !ok {
			return nil, errors.New("target '" + cq.Targets[idx].Type + "' is unsupported")
		}

		cb, err := create(cq, cq.Targets[idx].Arguments)
		if err != nil {
			return nil, err
		}
		cbList = append(cbList, cb)
	}

	cb := func(cq *service.ContinuousQuery, value interface{}) error {
		var errList []error
		for idx := range cbList {
			err := cbList[idx](cq, value)
			if err != nil {
				errList = append(errList, err)
			}
		}
		if len(errList) == 0 {
			return nil
		}
		return ekanite.ErrArray(errList)
	}
	cq.Callback = cb
	return cb, nil
}

func toHandler(cq *service.ContinuousQuery, cb CQHandleFunc) func(*bleve.SearchRequest, *bleve.SearchResult) error {
	return func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return cb(cq, resp)
	}
}

func toGroupByHandler(cq *service.ContinuousQuery, cb CQHandleFunc) func(map[string]uint64) error {
	return func(stats map[string]uint64) error {
		return cb(cq, stats)
	}
}
