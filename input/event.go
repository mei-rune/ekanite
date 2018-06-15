package input

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ekanite/ekanite"
)

var sequenceID int32

// Event is a log message, with a reception timestamp and sequence number.
type Event struct {
	Text          string                 // Delimited log line
	Parsed        map[string]interface{} // If non-nil, contains parsed fields
	ReceptionTime time.Time              // Time log line was received
	Sequence      int64                  // Provides order of reception
	SourceIP      string                 // Sender's IP address

	referenceTime time.Time // Memomized reference time
}

// ID returns a unique ID for the event.
func (e *Event) ID() ekanite.DocID {
	if e.Sequence == 0 {
		e.Sequence = int64(atomic.AddInt32(&sequenceID, 1))
	}
	return ekanite.DocID(fmt.Sprintf("%016x%016x",
		uint64(e.ReferenceTime().UnixNano()), uint64(e.Sequence)))
}

// Data returns the indexable data.
func (e *Event) Data() interface{} {
	return e.Parsed
}

// ReferenceTime returns the reference time of an event.
func (e *Event) ReferenceTime() time.Time {
	if e.referenceTime.IsZero() {
		if e.Parsed == nil {
			e.referenceTime = e.ReceptionTime
		} else if o, ok := e.Parsed["timestamp"]; !ok {
			e.referenceTime = e.ReceptionTime
		} else if ts, ok := o.(time.Time); ok {
			return ts
		} else if s, ok := o.(string); ok {
			if refTime, err := time.Parse(time.RFC3339, s); err != nil {
				e.referenceTime = e.ReceptionTime
			} else {
				e.referenceTime = refTime
			}
		} else {
			e.referenceTime = e.ReceptionTime
		}
	}
	return e.referenceTime
}
