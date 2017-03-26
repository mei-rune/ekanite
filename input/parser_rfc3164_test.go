package input

import (
	"testing"
	"time"
)

var (
	// XXX : corresponds to the length of the last tried timestamp format
	// XXX : Jan  2 15:04:05
	lastTriedTimestampLen = 15
)

func TestParsemessage_Valid(t *testing.T) {
	content := "foo bar baz blah quux"
	buff := []byte("sometag[123]: " + content)
	assertTag(t, buff, 14, "sometag")
}
func TestParseTimestamp_Invalid(t *testing.T) {
	buff := []byte("Oct 34 32:72:82")
	ts := new(time.Time)
	assertTimestamp(t, *ts, buff, lastTriedTimestampLen, ErrTimestampUnknownFormat)
}
func TestParseTimestamp_TrailingSpace(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct 11 22:14:15 ")
	now := time.Now()
	ts := time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC)
	assertTimestamp(t, ts, buff, len(buff), nil)
}
func TestParseTimestamp_OneDigitForMonths(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct  1 22:14:15")
	now := time.Now()
	ts := time.Date(now.Year(), time.October, 1, 22, 14, 15, 0, time.UTC)
	assertTimestamp(t, ts, buff, len(buff), nil)
}
func TestParseTimestamp_Valid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct 11 22:14:15")
	now := time.Now()
	ts := time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC)
	assertTimestamp(t, ts, buff, len(buff), nil)
}
func TestParseTag_Pid(t *testing.T) {
	buff := []byte("apache2[10]:")
	assertTag(t, buff, len(buff), "apache2")
}
func TestParseTag_NoPid(t *testing.T) {
	buff := []byte("apache2:")
	assertTag(t, buff, len(buff), "apache2")
}
func TestParseTag_TrailingSpace(t *testing.T) {
	buff := []byte("apache2: ")
	assertTag(t, buff, len(buff), "apache2")
}
func assertTimestamp(t *testing.T, ts time.Time, b []byte, excepted_cursor int, excepted_error error) {
	cursor, obtained, err := ParseTimestamp(b)
	if nil != err {
		if nil == excepted_error {
			t.Errorf("excepted is nil")
			t.Errorf("actual is %#v", err.Error())
			return
		}
		if excepted_error.Error() != err.Error() {
			t.Errorf("excepted is %#v", excepted_error.Error())
			t.Errorf("actual is %#v", err.Error())
		}
		return
	}
	AssertDeepEquals(t, "", obtained, ts)
	AssertDeepEquals(t, "", len(b)-len(cursor), excepted_cursor)
}
