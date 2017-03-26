package input

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
	"unicode"
)

const (
	NILVALUE = '-'
)

var (
	ErrYearInvalid       = &ParserError{"Invalid year in timestamp"}
	ErrMonthInvalid      = &ParserError{"Invalid month in timestamp"}
	ErrDayInvalid        = &ParserError{"Invalid day in timestamp"}
	ErrHourInvalid       = &ParserError{"Invalid hour in timestamp"}
	ErrMinuteInvalid     = &ParserError{"Invalid minute in timestamp"}
	ErrSecondInvalid     = &ParserError{"Invalid second in timestamp"}
	ErrSecFracInvalid    = &ParserError{"Invalid fraction of second in timestamp"}
	ErrTimeZoneInvalid   = &ParserError{"Invalid time zone in timestamp"}
	ErrInvalidTimeFormat = &ParserError{"Invalid time format"}
	ErrInvalidAppName    = &ParserError{"Invalid app name"}
	ErrInvalidProcId     = &ParserError{"Invalid proc ID"}
	ErrInvalidMsgId      = &ParserError{"Invalid msg ID"}
	ErrNoStructuredData  = &ParserError{"No structured data"}
)

// RFC5424V2 represents a parser for RFC5424V2-compliant log messages
type RFC5424V2 struct {
	matcher *regexp.Regexp
}

func (s *RFC5424V2) compileMatcher() {
	leading := `(?s)`
	pri := `<([0-9]{1,3})>`
	ver := `([0-9])`
	ts := `([^ ]+)`
	host := `([^ ]+)`
	app := `([^ ]+)`
	pid := `(-|[0-9]{1,5})`
	id := `([\w-]+)`
	msg := `(.+$)`
	s.matcher = regexp.MustCompile(leading + pri + ver + `\s` + ts + `\s` + host + `\s` + app + `\s` + pid + `\s` + id + `\s` + msg)
}

func (s *RFC5424V2) parse(raw []byte, result *map[string]interface{}) {
	m := s.matcher.FindStringSubmatch(string(raw))
	if m == nil || len(m) != 9 {
		stats.Add("rfc5424Unparsed", 1)
		return
	}
	stats.Add("rfc5424Parsed", 1)
	pri, _ := strconv.Atoi(m[1])
	ver, _ := strconv.Atoi(m[2])
	var pid int
	if m[6] != "-" {
		pid, _ = strconv.Atoi(m[6])
	}
	*result = map[string]interface{}{
		"priority":   pri,
		"version":    ver,
		"timestamp":  m[3],
		"host":       m[4],
		"app":        m[5],
		"pid":        pid,
		"message_id": m[7],
		"message":    m[8],
	}
}

type rfc5424 struct {
}

// HEADER = PRI VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID
func (p *rfc5424) Parse(bs []byte) (map[string]interface{}, error) {
	next, pri, err := ParsePriority(bs)
	if err != nil {
		if '"' == bs[0] {
			next, pri, err = ParsePriority(bs[1:]) // p.parsePriority()
			if err != nil {
				ts := time.Now()
				return map[string]interface{}{
					"priority":  0,
					"facility":  0,
					"severity":  0,
					"version":   NO_VERSION,
					"timestamp": ToJavaTime(ts),
					"message":   string(bs),
				}, nil
			}

			if bs[len(bs)-1] == '"' && next[len(next)-1] == '"' {
				next = next[:len(next)-1]
			}
		}
	}
	// fmt.Println("====1", string(next))
	next, version, _ := ParseVersion(next)
	// fmt.Println("====2", string(next))
	next, ts, _ := p.parseTimestamp(next)
	// fmt.Println("====3", string(next), ts)
	var hostname, appName, msgId, sd string
	var procId interface{}
	if !ts.IsZero() {
		next, hostname = ParseHostname(next)
		//fmt.Println("====4", string(next))
		next, appName, _ = p.parseAppName(next)
		//fmt.Println("====5", string(next))
		next, procId, _ = p.parseProcId(next)
		//fmt.Println("====6", string(next))
		next, msgId, _ = p.parseMsgId(next)
		//fmt.Println("====7", string(next))
		next, sd, _ = p.parseStructuredData(next)
		//fmt.Println("====7", string(next))
	}
	message := bytes.TrimSpace(next)
	if ts.IsZero() {
		ts = time.Now()
	}

	//result := sd
	//if result == nil {
	result := map[string]interface{}{}
	//}
	result["priority"] = pri.P
	result["facility"] = pri.F.Value
	result["severity"] = pri.S.Value
	result["version"] = version
	result["timestamp"] = ToJavaTime(ts)
	result["host"] = hostname
	result["app"] = appName
	result["pid"] = procId
	result["message_id"] = msgId
	result["structured_data"] = sd
	result["message"] = string(message)
	return result, nil
}

// https://tools.ietf.org/html/rfc5424#section-6.2.3
func (p *rfc5424) parseTimestamp(bs []byte) ([]byte, time.Time, error) {
	var ts time.Time
	if len(bs) <= 0 {
		return bs, ts, nil
	}
	var to int
	for to = 0; to < len(bs); to++ {
		if !unicode.IsSpace(rune(bs[to])) {
			break
		}
	}
	if bs[to] == '-' {
		return bs[to+1:], ts, nil
	}
	next, fd, err := parseFullDate(bs[to:])
	if err != nil {
		return bs, ts, err
	}
	if next[0] != 'T' {
		return bs, ts, ErrInvalidTimeFormat
	}
	next, ft, err := parseFullTime(next[1:])
	if err != nil {
		return bs, ts, ErrTimestampUnknownFormat
	}
	nSec, _ := toNSec(ft.pt.secFrac)
	ts = time.Date(
		fd.year,
		time.Month(fd.month),
		fd.day,
		ft.pt.hour,
		ft.pt.minute,
		ft.pt.seconds,
		nSec,
		ft.loc,
	)
	return next, ts, nil
}

// APP-NAME = NILVALUE / 1*48PRINTUSASCII
func (p *rfc5424) parseAppName(bs []byte) ([]byte, string, error) {
	return parseUpToLen(bs, 48, ErrInvalidAppName)
}

// PROCID = NILVALUE / 1*128PRINTUSASCII
func (p *rfc5424) parseProcId(bs []byte) ([]byte, interface{}, error) {
	next, app, err := parseUpToLen(bs, 128, ErrInvalidProcId)
	if err != nil {
		return bs, -1, err
	}
	i64, err := strconv.ParseInt(app, 10, 0)
	if err != nil {
		if app == "-" {
			return next, -1, nil
		}
		return next, app, nil
	}
	return next, int(i64), nil
}

// MSGID = NILVALUE / 1*32PRINTUSASCII
func (p *rfc5424) parseMsgId(bs []byte) ([]byte, string, error) {
	return parseUpToLen(bs, 32, ErrInvalidMsgId)
}
func (p *rfc5424) parseStructuredData(bs []byte) ([]byte, string, error) {
	return parseStructuredData(bs)
}

type partialTime struct {
	hour    int
	minute  int
	seconds int
	secFrac float64
}
type fullTime struct {
	pt  partialTime
	loc *time.Location
}
type fullDate struct {
	year  int
	month int
	day   int
}

// ----------------------------------------------
// https://tools.ietf.org/html/rfc5424#section-6
// ----------------------------------------------
// XXX : bind them to rfc5424 ?
// FULL-DATE : DATE-FULLYEAR "-" DATE-MONTH "-" DATE-MDAY
func parseFullDate(bs []byte) ([]byte, fullDate, error) {
	var fd fullDate
	next, year, err := parseYear(bs)
	if err != nil {
		return bs, fd, err
	}
	if next[0] != '-' {
		//fmt.Println(string(next))
		return bs, fd, ErrTimestampUnknownFormat
	}
	next, month, err := parseMonth(next[1:])
	if err != nil {
		//fmt.Println(string(next))
		return bs, fd, err
	}
	if next[0] != '-' {
		//fmt.Println(string(next))
		return bs, fd, ErrTimestampUnknownFormat
	}
	next, day, err := parseDay(next[1:])
	if err != nil {
		//fmt.Println(string(next))
		return bs, fd, err
	}
	fd = fullDate{
		year:  year,
		month: month,
		day:   day,
	}
	return next, fd, nil
}

// DATE-FULLYEAR   = 4DIGIT
func parseYear(bs []byte) ([]byte, int, error) {
	yearLen := 4
	if len(bs) < yearLen {
		return bs, 0, ErrEOL
	}
	// XXX : we do not check for a valid year (ie. 1999, 2013 etc)
	// XXX : we only checks the format is correct
	year, err := strconv.Atoi(string(bs[:yearLen]))
	if err != nil {
		return bs, 0, ErrYearInvalid
	}
	return bs[yearLen:], year, nil
}

// DATE-MONTH = 2DIGIT  ; 01-12
func parseMonth(bs []byte) ([]byte, int, error) {
	return Parse2Digits(bs, 1, 12, ErrMonthInvalid)
}

// DATE-MDAY = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on month/year
func parseDay(bs []byte) ([]byte, int, error) {
	// XXX : this is a relaxed constraint
	// XXX : we do not check if valid regarding February or leap years
	// XXX : we only checks that day is in range [01 -> 31]
	// XXX : in other words this function will not rant if you provide Feb 31th
	return Parse2Digits(bs, 1, 31, ErrDayInvalid)
}

// FULL-TIME = PARTIAL-TIME TIME-OFFSET
func parseFullTime(bs []byte) ([]byte, fullTime, error) {
	next, pt, err := parsePartialTime(bs)
	if err != nil {
		return bs, fullTime{}, err
	}
	//fmt.Println(string(next))
	next, loc, err := parseTimeOffset(next)
	if err != nil {
		loc = time.Local
		//return bs, fullTime{}, err
	}
	return next, fullTime{
		pt:  pt,
		loc: loc,
	}, nil
}

// PARTIAL-TIME = TIME-HOUR ":" TIME-MINUTE ":" TIME-SECOND[TIME-SECFRAC]
func parsePartialTime(bs []byte) ([]byte, partialTime, error) {
	var pt partialTime
	next, hour, err := parseHour(bs)
	if err != nil {
		//fmt.Println("===========1", string(bs), err)
		return bs, pt, err
	}
	if next[0] != ':' {
		//fmt.Println("===========2", string(next), err)
		return bs, pt, ErrInvalidTimeFormat
	}
	next, minute, err := parseMinute(next[1:])
	if err != nil {
		//fmt.Println("===========3", string(next), err)
		return bs, pt, err
	}
	if next[0] != ':' {
		//fmt.Println("===========4", string(next), err)
		return bs, pt, ErrInvalidTimeFormat
	}
	next, seconds, err := parseSecond(next[1:])
	if err != nil {
		//fmt.Println("===========5", string(next), err)
		return bs, pt, err
	}
	pt = partialTime{
		hour:    hour,
		minute:  minute,
		seconds: seconds,
	}
	if next[0] != '.' {
		return next, pt, nil
	}
	next, secFrac, err := parseSecFrac(next[1:])
	if err != nil {
		return next, pt, nil
	}
	pt.secFrac = secFrac
	return next, pt, nil
}

// TIME-HOUR = 2DIGIT  ; 00-23
func parseHour(bs []byte) ([]byte, int, error) {
	return Parse2Digits(bs, 0, 23, ErrHourInvalid)
}

// TIME-MINUTE = 2DIGIT  ; 00-59
func parseMinute(bs []byte) ([]byte, int, error) {
	return Parse2Digits(bs, 0, 59, ErrMinuteInvalid)
}

// TIME-SECOND = 2DIGIT  ; 00-59
func parseSecond(bs []byte) ([]byte, int, error) {
	return Parse2Digits(bs, 0, 59, ErrSecondInvalid)
}

// TIME-SECFRAC = "." 1*6DIGIT
func parseSecFrac(bs []byte) ([]byte, float64, error) {
	maxDigitLen := 9
	to := 0
	for to = 0; to < maxDigitLen; to++ {
		if to >= len(bs) {
			break
		}
		if !IsDigit(bs[to]) {
			break
		}
	}
	if to == 0 {
		return bs, 0, ErrSecFracInvalid
	}
	secFrac, err := strconv.ParseFloat("0."+string(bs[:to]), 64)
	if err != nil {
		return bs, 0, ErrSecFracInvalid
	}
	return bs[to:], secFrac, nil
}

// TIME-OFFSET = "Z" / TIME-NUMOFFSET
func parseTimeOffset(bs []byte) ([]byte, *time.Location, error) {
	if bs[0] == 'Z' {
		if len(bs) >= 2 && unicode.IsSpace(rune(bs[1])) {
			return bs[2:], time.UTC, nil
		}
		return bs[1:], time.UTC, nil
	}
	return parseNumericalTimeOffset(bs)
}

// TIME-NUMOFFSET  = ("+" / "-") TIME-HOUR ":" TIME-MINUTE
func parseNumericalTimeOffset(bs []byte) ([]byte, *time.Location, error) {
	if (bs[0] != '+') && (bs[0] != '-') {
		return bs, time.UTC, ErrTimeZoneInvalid
	}
	next, hour, err := parseHour(bs[1:])
	if err != nil {
		return bs, time.UTC, err
	}
	if next[0] != ':' {
		return bs, time.UTC, ErrInvalidTimeFormat
	}
	next, minute, err := parseMinute(next[1:])
	if err != nil {
		return bs, time.UTC, err
	}
	tmp, err := time.Parse("-07:00", fmt.Sprintf("%s%02d:%02d", string(bs[0]), hour, minute))
	if err != nil {
		return bs, time.UTC, err
	}
	return next, tmp.Location(), nil
}
func toNSec(sec float64) (int, error) {
	_, frac := math.Modf(sec)
	fracStr := strconv.FormatFloat(frac, 'f', 9, 64)
	fracInt, err := strconv.Atoi(fracStr[2:])
	if err != nil {
		return 0, err
	}
	return fracInt, nil
}

// ------------------------------------------------
// https://tools.ietf.org/html/rfc5424#section-6.3
// ------------------------------------------------
func parseStructuredData(bs []byte) ([]byte, string, error) {
	to := 0
	for {
		if to >= len(bs) {
			return bs, "-", ErrEOL
		}
		if !unicode.IsSpace(rune(bs[to])) {
			break
		}
		to++
	}
	if bs[to] != '[' {
		if bs[to] == '-' {
			return bs[to+1:], "-", nil
		}
		return bs, "-", ErrNoStructuredData
	}
	from := to
	for ; to < len(bs); to++ {
		if bs[to] == ']' {
			if len(bs) <= to+1 {
				return bs[len(bs):], string(bs[from : to+1]), nil
			}
			if bs[to+1] == ' ' {
				return bs[to+1:], string(bs[from : to+1]), nil
			}
		}
	}
	return bs, "-", ErrNoStructuredData
}
func parseUpToLen(bs []byte, maxLen int, e error) ([]byte, string, error) {
	to := 0
	for ; ; to++ {
		if to >= maxLen {
			return bs, "", e
		}
		if to >= len(bs) {
			return bs, "", e
		}
		if !unicode.IsSpace(rune(bs[to])) {
			break
		}
	}
	from := to
	for ; (to < maxLen) && (to < len(bs)); to++ {
		if unicode.IsSpace(rune(bs[to])) {
			return bs[to:], string(bs[from:to]), nil
		}
	}
	return bs, "", e
}
func rfc6587ScannerSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ' '); i > 0 {
		pLength := data[0:i]
		length, err := strconv.Atoi(string(pLength))
		if err != nil {
			return 0, nil, err
		}
		end := length + i + 1
		if len(data) >= end {
			//Return the frame with the length removed
			return end, data[i+1 : end], nil
		}
	}
	// Request more data
	return 0, nil, nil
}
