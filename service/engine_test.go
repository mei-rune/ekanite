package service

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
)

// tempPath provides a path for temporary use.
func tempPath() string {
	f, _ := ioutil.TempFile("", "ekanite_")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func newEngine(path string, numShards int, indexDuration time.Duration) *ekanite.Engine {
	e := ekanite.NewEngine(path)
	e.Open()
	e.NumShards = numShards
	e.IndexDuration = indexDuration
	return e
}

func TestEngine_IndexMapThenSearch(t *testing.T) {
	dataDir := tempPath()
	defer os.RemoveAll(dataDir)
	e := ekanite.NewEngine(dataDir)

	ev1 := newMapEvent(ParseTime("1982-02-05T04:43:00Z"), map[string]interface{}{
		"address":  "127.0.0.1",
		"message":  "auth password accepted for user philip",
		"severity": 1,
		"facility": 2,
	})
	ev2 := newMapEvent(ParseTime("1982-02-05T04:43:01Z"), map[string]interface{}{
		"address":  "192.168.1.2",
		"message":  "auth password accepted for user root",
		"severity": 4,
		"facility": 2,
	})
	ev3 := newMapEvent(ParseTime("1982-02-05T04:43:02Z"), map[string]interface{}{
		"address":  "192.168.1.5",
		"message":  "auth password accepted for user robot",
		"severity": 6,
		"facility": 2,
	})

	if err := e.Index([]ekanite.Document{ev1, ev2, ev3}); err != nil {
		t.Fatalf("failed to index events: %s", err.Error())
	}
	total, err := e.Total()
	if err != nil {
		t.Errorf("failed to get engine total doc count: %s", err.Error())
	}
	if total != 3 {
		t.Errorf("engine total doc count, got %d, expected 3", total)
	}

	// searchIn(e, time.Time{}, time.Time{}, searchRequest*bleve.SearchRequest,
	// 	func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {

	// 	})

}

// Event is a log message, with a reception timestamp and sequence number.
type mapEvent struct {
	ReceptionTime time.Time // Time log line was received
	Sequence      int64     // Provides order of reception
	Fields        map[string]interface{}
}

// ID returns a unique ID for the event.
func (e *mapEvent) ID() ekanite.DocID {
	return ekanite.DocID(fmt.Sprintf("%016x%016x",
		uint64(e.ReferenceTime().UnixNano()), uint64(e.Sequence)))
}

// Data returns the indexable data.
func (e *mapEvent) Data() interface{} {
	return e.Fields
}

// ReferenceTime returns the reference time of an event.
func (e *mapEvent) ReferenceTime() time.Time {
	return e.ReceptionTime
}

var sequence int64 = 0

func newMapEvent(refTime time.Time, fields map[string]interface{}) ekanite.Document {
	sequence++
	fields["reception"] = refTime
	return &mapEvent{
		Fields:        fields,
		ReceptionTime: refTime,
		Sequence:      sequence,
	}
}

func searchIn(searcher ekanite.Searcher, start, end time.Time, searchRequest *bleve.SearchRequest,
	cb func(req *bleve.SearchRequest, resp *bleve.SearchResult) error) error {
	if !start.IsZero() || !end.IsZero() {
		inclusive := true
		timeQuery := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		timeQuery.SetField("reception")

		if searchRequest.Query == nil {
			searchRequest.Query = timeQuery
		} else if conjunctionQuery, ok := searchRequest.Query.(*query.ConjunctionQuery); ok {
			conjunctionQuery.AddQuery(timeQuery)
		} else {
			searchRequest.Query = bleve.NewConjunctionQuery(searchRequest.Query, timeQuery)
		}
	} else if searchRequest.Query == nil {
		inclusive := true
		timeQuery := bleve.NewDateRangeInclusiveQuery(start, time.Now(), &inclusive, &inclusive)
		timeQuery.SetField("reception")

		searchRequest.Query = timeQuery
	}

	//bs, _ := json.Marshal(searchRequest)
	//fmt.Printf("parsed request: %s\r\n", string(bs))

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			return err
		}
	}

	// execute the query
	return searcher.Query(start, end, searchRequest, cb)
}
