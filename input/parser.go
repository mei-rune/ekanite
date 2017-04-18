package input

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	PRI_PART_START = '<'
	PRI_PART_END   = '>'
	NO_VERSION     = -1
)

var (
	ErrEOL                    = &ParserError{"End of log line"}
	ErrNoSpace                = &ParserError{"No space found"}
	ErrPriorityNoStart        = &ParserError{"No start char found for priority"}
	ErrPriorityEmpty          = &ParserError{"Priority field empty"}
	ErrPriorityNoEnd          = &ParserError{"No end char found for priority"}
	ErrPriorityTooShort       = &ParserError{"Priority field too short"}
	ErrPriorityTooLong        = &ParserError{"Priority field too long"}
	ErrPriorityNonDigit       = &ParserError{"Non digit found in priority"}
	ErrVersionNotFound        = &ParserError{"Can not find version"}
	ErrTimestampUnknownFormat = &ParserError{"Timestamp format unknown"}
)

var (
	fmtsByStandard = []string{"rfc5424", "rfc3164", "syslog"}
)

// ValidFormat returns if the given format matches one of the possible formats.
func ValidFormat(format string) bool {
	for _, f := range fmtsByStandard {
		if f == format {
			return true
		}
	}
	return false
}

// A Parser parses the raw input as a map with a timestamp field.
type LogParser struct {
	fmt    string
	Raw    []byte
	Result map[string]interface{}
	//rfc5424 *RFC5424
	formatByAddress map[string]func() Parser
}

// NewParser returns a new Parser instance.
func NewLogParser(f string) (*LogParser, error) {
	if !ValidFormat(f) {
		return nil, fmt.Errorf("%s is not a valid format", f)
	}

	formatByAddress := map[string]func() Parser{}
	p := &LogParser{formatByAddress: formatByAddress}
	p.detectFmt(strings.TrimSpace(strings.ToLower(f)))
	//p.newRFC5424Parser()
	return p, nil
}

// Reads the given format and detects its internal name.
func (p *LogParser) detectFmt(f string) {
	for _, v := range fmtsByStandard {
		if f == v {
			p.fmt = v
			return
		}
	}
	stats.Add("invalidParserFormat", 1)
	p.fmt = fmtsByStandard[0]
	return
}

// Parse the given byte slice.
func (p *LogParser) Parse(address string, b []byte) {
	//p.Result = map[string]interface{}{}
	p.Raw = b
	var r Parser

	if format := p.formatByAddress[address]; format != nil {
		r = format()
	} else {
		r = CreateParser(p.fmt)
	}
	p.Result, err = r.Parse(b)
	if err != nil {
		p.Result = map[string]interface{}{
			"priority":  0,
			"facility":  0,
			"severity":  0,
			"version":   NO_VERSION,
			"timestamp": time.Now(),
			"message":   string(b),
		}
	}
}

type Parser interface {
	Parse(bs []byte) (map[string]interface{}, error)
}

func CreateParser(format string) Parser {
	switch strings.ToLower(format) {
	case "rfc5424":
		return &rfc5424{}
	case "rfc3164":
		return &rfc3164{year: strconv.FormatInt(int64(time.Now().Year()), 10)}
	default:
		return &rfc5424{}
	}
}

type ParserError struct {
	ErrorString string
}

func (err *ParserError) Error() string {
	return err.ErrorString
}

type Priority struct {
	P int
	F Facility
	S Severity
}
type Facility struct {
	Value int
}
type Severity struct {
	Value int
}

// https://tools.ietf.org/html/rfc3164#section-4.1
func ParsePriority(bs []byte) ([]byte, Priority, error) {
	pri := newPriority(0)
	if len(bs) <= 0 {
		return bs, pri, ErrPriorityEmpty
	}
	if bs[0] != PRI_PART_START {
		return bs, pri, ErrPriorityNoStart
	}
	i := 1
	priDigit := 0
	for i = 1; i < len(bs); i++ {
		if i >= 5 {
			return bs, pri, ErrPriorityTooLong
		}
		c := bs[i]
		if c == PRI_PART_END {
			if i == 1 {
				return bs, pri, ErrPriorityTooShort
			}
			return bs[i+1:], newPriority(priDigit), nil
		}
		v, e := strconv.Atoi(string(c))
		if e != nil {
			return bs, pri, ErrPriorityNonDigit
		}
		priDigit = (priDigit * 10) + v
	}
	return bs, pri, ErrPriorityNoEnd
}

func skipSpace(bs []byte) []byte {
	for to := 0; to < len(bs); to++ {
		if !unicode.IsSpace(rune(bs[to])) {
			return bs[to:]
		}
	}
	return bs[len(bs):]
}

// https://tools.ietf.org/html/rfc5424#section-6.2.2
func ParseVersion(bs []byte) ([]byte, int, error) {
	next := skipSpace(bs)
	if len(next) <= 0 {
		return bs, NO_VERSION, ErrVersionNotFound
	}
	c := next[0]
	v, e := strconv.Atoi(string(c))
	if e != nil {
		return bs, NO_VERSION, ErrVersionNotFound
	}
	if len(next) >= 2 {
		if !unicode.IsSpace(rune(next[1])) {
			return bs, NO_VERSION, ErrVersionNotFound
		}
	}
	return next[1:], v, nil
}
func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}
func newPriority(p int) Priority {
	// The Priority value is calculated by first multiplying the Facility
	// number by 8 and then adding the numerical value of the Severity.
	return Priority{
		P: p,
		F: Facility{Value: p / 8},
		S: Severity{Value: p % 8},
	}
}
func Parse2Digits(bs []byte, min int, max int, e error) ([]byte, int, error) {
	digitLen := 2
	if len(bs) < digitLen {
		return bs, 0, ErrEOL
	}
	i, err := strconv.Atoi(string(bs[:digitLen]))
	if err != nil {
		return bs, 0, e
	}
	if i >= min && i <= max {
		return bs[digitLen:], i, nil
	}
	return bs, 0, e
}
func fixTimestampIfNeeded(ts *time.Time) {
	now := time.Now()
	y := ts.Year()
	if ts.Year() == 0 {
		y = now.Year()
	}
	newTs := time.Date(y, ts.Month(), ts.Day(), ts.Hour(), ts.Minute(),
		ts.Second(), ts.Nanosecond(), ts.Location())
	*ts = newTs
}

var (
	tsFmts = []string{
		"Jan 02 15:04:05 2006",
		"Jan  2 15:04:05 2006",
		"Jan 2 15:04:05 2006",
		"Jan 02 15:04:05",
		"Jan  2 15:04:05",
		"Jan 2 15:04:05",
	}
	shortMonthNames = [][]byte{
		[]byte("Jan"),
		[]byte("Feb"),
		[]byte("Mar"),
		[]byte("Apr"),
		[]byte("May"),
		[]byte("Jun"),
		[]byte("Jul"),
		[]byte("Aug"),
		[]byte("Sep"),
		[]byte("Oct"),
		[]byte("Nov"),
		[]byte("Dec"),
	}
)

// ParseTimestamp https://tools.ietf.org/html/rfc3164#section-4.1.2
func ParseTimestamp(bs []byte) ([]byte, time.Time, error) {
	var ts time.Time
	var err error
	var tsFmtLen int

	bs = skipSpace(bs)
	found := false
	for _, tsFmt := range tsFmts {
		tsFmtLen = len(tsFmt)
		if tsFmtLen > len(bs) {
			continue
		}
		ts, err = time.Parse(tsFmt, string(bs[:tsFmtLen]))
		if err == nil {
			found = true
			break
		}
	}
	if !found {
		found := false
		for _, s := range shortMonthNames {
			if bytes.HasPrefix(bs, s) {
				found = true
				break
			}
		}
		if !found {
			return bs, ts, ErrTimestampUnknownFormat
		}
		fields := bytes.Fields(bs)
		if len(fields) < 3 {
			return bs, ts, ErrTimestampUnknownFormat
		}

		s := string(fields[0]) + " " + string(fields[1]) + " " + string(fields[2])
		for _, tsFmt := range tsFmts {
			ts, err = time.Parse(tsFmt, s)
			if err == nil {
				fixTimestampIfNeeded(&ts)
				return bytes.Join(fields[3:], []byte(" ")), ts, nil
			}
		}

		//p.cursor = tsFmtLen
		// XXX : If the timestamp is invalid we try to push the cursor one byte
		// XXX : further, in case it is a space
		//if tsFmtLen < len(bs) && unicode.IsSpace(bs[0]) {
		//	tsFmtLen + 1
		//}
		return bs, ts, ErrTimestampUnknownFormat
	}
	fixTimestampIfNeeded(&ts)
	if tsFmtLen < len(bs) && unicode.IsSpace(rune(bs[tsFmtLen])) {
		tsFmtLen += 1
	}
	return bs[tsFmtLen:], ts, nil
}

func ParseHostname(bs []byte) ([]byte, string) {
	var to int
	for to = 0; to < len(bs); to++ {
		if !unicode.IsSpace(rune(bs[to])) {
			break
		}
	}
	if bs[to] == '-' {
		return bytes.TrimSpace(bs[to+1:]), ""
	}
	from := to
	for ; to < len(bs); to++ {
		if unicode.IsSpace(rune(bs[to])) {
			return bytes.TrimSpace(bs[to:]), string(bs[from:to])
		}
	}
	return bs, ""
}

// http://tools.ietf.org/html/rfc3164#section-4.1.3
func ParseTag(bs []byte) ([]byte, string) {
	var endOfTag bool
	var bracketOpen bool
	var tag []byte
	var found bool
	to := 0
	for {
		if to >= len(bs) {
			return bs, ""
		}
		if !unicode.IsSpace(rune(bs[to])) {
			break
		}
		to++
	}
	from := to
	for to < len(bs) {
		b := bs[to]
		bracketOpen = (b == '[')
		endOfTag = (b == ':' || b == ' ')
		// XXX : parse PID ?
		if bracketOpen {
			tag = bs[from:to]
			found = true
		}
		if endOfTag {
			if !found {
				tag = bs[from:to]
				found = true
			}
			to++
			break
		}
		to++
	}
	if (to < len(bs)) && unicode.IsSpace(rune(bs[to])) {
		to++
	}
	if found {
		return bs[to:], string(tag)
	} else {
		return bs, ""
	}
}

func ShowCursorPos(buff []byte, cursor int) {
	fmt.Println(string(buff))
	padding := strings.Repeat("-", cursor)
	fmt.Println(padding + "↑\n")
}
