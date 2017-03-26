package input

import (
	"testing"
	"time"
)

func TestParseTimestamp_UTC(t *testing.T) {
	buff := []byte("1985-04-12T23:20:50.52Z")
	ts := time.Date(1985, time.April, 12, 23, 20, 50, 52*10e6, time.UTC)
	assertRfc5424Timestamp(t, ts, buff, 23, nil)
}
func TestParseTimestamp_NumericTimezone(t *testing.T) {
	tz := "-04:00"
	buff := []byte("1985-04-12T19:20:50.52" + tz)
	tmpTs, err := time.Parse("-07:00", tz)
	AssertIsNil(t, err)
	ts := time.Date(1985, time.April, 12, 19, 20, 50, 52*10e6, tmpTs.Location())
	assertRfc5424Timestamp(t, ts, buff, len(buff), nil)
}
func TestParseTimestamp_MilliSeconds(t *testing.T) {
	buff := []byte("2003-10-11T22:14:15.003Z")
	ts := time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)
	assertRfc5424Timestamp(t, ts, buff, len(buff), nil)
}
func TestParseTimestamp_MicroSeconds(t *testing.T) {
	tz := "-07:00"
	buff := []byte("2003-08-24T05:14:15.000003" + tz)
	tmpTs, err := time.Parse("-07:00", tz)
	AssertIsNil(t, err)
	ts := time.Date(2003, time.August, 24, 5, 14, 15, 3*10e2, tmpTs.Location())
	assertRfc5424Timestamp(t, ts, buff, len(buff), nil)
}
func TestParseTimestamp_NanoSeconds(t *testing.T) {
	buff := []byte("2003-08-24T05:14:15.000000003-07:00")
	ts := time.Date(2003, time.Month(8), 24, 05, 14, 15, 000000003, time.FixedZone("-07:00", -7*60*60))
	assertRfc5424Timestamp(t, ts, buff, 35, ErrTimestampUnknownFormat)
}
func TestParseTimestamp_NilValue(t *testing.T) {
	buff := []byte("-")
	ts := new(time.Time)
	assertRfc5424Timestamp(t, *ts, buff, 1, nil)
}
func TestParseYear_Invalid(t *testing.T) {
	buff := []byte("1a2b")
	expected := 0
	assertParseYear(t, expected, buff, 0, ErrYearInvalid)
}
func TestParseYear_TooShort(t *testing.T) {
	buff := []byte("123")
	expected := 0
	assertParseYear(t, expected, buff, 0, ErrEOL)
}
func TestParseYear_Valid(t *testing.T) {
	buff := []byte("2013")
	expected := 2013
	assertParseYear(t, expected, buff, 4, nil)
}
func TestParseMonth_InvalidString(t *testing.T) {
	buff := []byte("ab")
	expected := 0
	assertParseMonth(t, expected, buff, 0, ErrMonthInvalid)
}
func TestParseMonth_InvalidRange(t *testing.T) {
	buff := []byte("00")
	expected := 0
	assertParseMonth(t, expected, buff, 0, ErrMonthInvalid)
	// ----
	buff = []byte("13")
	assertParseMonth(t, expected, buff, 0, ErrMonthInvalid)
}
func TestParseMonth_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0
	assertParseMonth(t, expected, buff, 0, ErrEOL)
}
func TestParseMonth_Valid(t *testing.T) {
	buff := []byte("02")
	expected := 2
	assertParseMonth(t, expected, buff, 2, nil)
}
func TestParseDay_InvalidString(t *testing.T) {
	buff := []byte("ab")
	expected := 0
	assertParseDay(t, expected, buff, 0, ErrDayInvalid)
}
func TestParseDay_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0
	assertParseDay(t, expected, buff, 0, ErrEOL)
}
func TestParseDay_InvalidRange(t *testing.T) {
	buff := []byte("00")
	expected := 0
	assertParseDay(t, expected, buff, 0, ErrDayInvalid)
	// ----
	buff = []byte("32")
	assertParseDay(t, expected, buff, 0, ErrDayInvalid)
}
func TestParseDay_Valid(t *testing.T) {
	buff := []byte("02")
	expected := 2
	assertParseDay(t, expected, buff, 2, nil)
}
func TestParseFullDate_Invalid(t *testing.T) {
	buff := []byte("2013+10-28")
	fd := fullDate{}
	assertParseFullDate(t, fd, buff, 0, ErrTimestampUnknownFormat)
	// ---
	buff = []byte("2013-10+28")
	assertParseFullDate(t, fd, buff, 0, ErrTimestampUnknownFormat)
}
func TestParseFullDate_Valid(t *testing.T) {
	buff := []byte("2013-10-28")
	fd := fullDate{
		year:  2013,
		month: 10,
		day:   28,
	}
	assertParseFullDate(t, fd, buff, len(buff), nil)
}
func TestParseHour_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0
	assertParseHour(t, expected, buff, 0, ErrHourInvalid)
}
func TestParseHour_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0
	assertParseHour(t, expected, buff, 0, ErrEOL)
}
func TestParseHour_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0
	assertParseHour(t, expected, buff, 0, ErrHourInvalid)
	// ----
	buff = []byte("24")
	assertParseHour(t, expected, buff, 0, ErrHourInvalid)
}
func TestParseHour_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12
	assertParseHour(t, expected, buff, 2, nil)
}
func TestParseMinute_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0
	assertParseMinute(t, expected, buff, 0, ErrMinuteInvalid)
}
func TestParseMinute_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0
	assertParseMinute(t, expected, buff, 0, ErrEOL)
}
func TestParseMinute_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0
	assertParseMinute(t, expected, buff, 0, ErrMinuteInvalid)
	// ----
	buff = []byte("60")
	assertParseMinute(t, expected, buff, 0, ErrMinuteInvalid)
}
func TestParseMinute_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12
	assertParseMinute(t, expected, buff, 2, nil)
}
func TestParseSecond_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0
	assertParseSecond(t, expected, buff, 0, ErrSecondInvalid)
}
func TestParseSecond_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0
	assertParseSecond(t, expected, buff, 0, ErrEOL)
}
func TestParseSecond_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0
	assertParseSecond(t, expected, buff, 0, ErrSecondInvalid)
	// ----
	buff = []byte("60")
	assertParseSecond(t, expected, buff, 0, ErrSecondInvalid)
}
func TestParseSecond_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12
	assertParseSecond(t, expected, buff, 2, nil)
}
func TestParseSecFrac_InvalidString(t *testing.T) {
	buff := []byte("azerty")
	expected := 0.0
	assertParseSecFrac(t, expected, buff, 0, ErrSecFracInvalid)
}
func TestParseSecFrac_NanoSeconds(t *testing.T) {
	buff := []byte("12345678901")
	expected := 0.123456789
	assertParseSecFrac(t, expected, buff, 9, nil)
}
func TestParseSecFrac_Valid(t *testing.T) {
	buff := []byte("0")
	expected := 0.0
	assertParseSecFrac(t, expected, buff, 1, nil)
	buff = []byte("52")
	expected = 0.52
	assertParseSecFrac(t, expected, buff, 2, nil)
	buff = []byte("003")
	expected = 0.003
	assertParseSecFrac(t, expected, buff, 3, nil)
	buff = []byte("000003")
	expected = 0.000003
	assertParseSecFrac(t, expected, buff, 6, nil)
}
func TestParseNumericalTimeOffset_Valid(t *testing.T) {
	buff := []byte("+02:00")
	tmpTs, err := time.Parse("-07:00", string(buff))
	AssertIsNil(t, err)
	cursor, obtained, err := parseNumericalTimeOffset(buff)
	AssertIsNil(t, err)
	expected := tmpTs.Location()
	AssertDeepEquals(t, "", obtained, expected)
	AssertDeepEquals(t, "", len(buff)-len(cursor), 6)
}
func TestParseTimeOffset_Valid(t *testing.T) {
	buff := []byte("Z")
	cursor, obtained, err := parseTimeOffset(buff)
	AssertIsNil(t, err)
	AssertDeepEquals(t, "", obtained, time.UTC)
	AssertDeepEquals(t, "", len(buff)-len(cursor), 1)
}

// func TestGetHourMin_Valid(t *testing.T) {
// 	buff := []byte("12:34")
// 	cursor := 0
// 	l := len(buff)
//
// 	expectedHour := 12
// 	expectedMinute := 34
//
// 	cursor, obtainedHour, obtainedMinute, err := getHourMinute(buff)
// 	AssertIsNil(t, err)
// 	c.Assert(obtainedHour, Equals, expectedHour)
// 	c.Assert(obtainedMinute, Equals, expectedMinute)
//
// 	AssertDeepEquals(t, "", cursor, l)
// }
func TestParsePartialTime_Valid(t *testing.T) {
	buff := []byte("05:14:15.000003")
	l := len(buff)
	cursor, obtained, err := parsePartialTime(buff)
	expected := partialTime{
		hour:    5,
		minute:  14,
		seconds: 15,
		secFrac: 0.000003,
	}
	AssertIsNil(t, err)
	AssertDeepEquals(t, "", obtained, expected)
	AssertDeepEquals(t, "", len(buff)-len(cursor), l)
}
func TestParseFullTime_Valid(t *testing.T) {
	tz := "-02:00"
	buff := []byte("05:14:15.000003" + tz)
	tmpTs, err := time.Parse("-07:00", string(tz))
	AssertIsNil(t, err)
	cursor, obtainedFt, err := parseFullTime(buff)
	expectedFt := fullTime{
		pt: partialTime{
			hour:    5,
			minute:  14,
			seconds: 15,
			secFrac: 0.000003,
		},
		loc: tmpTs.Location(),
	}
	AssertIsNil(t, err)
	AssertDeepEquals(t, "", obtainedFt, expectedFt)
	AssertDeepEquals(t, "", len(buff)-len(cursor), 21)
}
func TestToNSec(t *testing.T) {
	fixtures := []float64{
		0.52,
		0.003,
		0.000003,
	}
	expected := []int{
		520000000,
		3000000,
		3000,
	}
	AssertDeepEquals(t, "", len(fixtures), len(expected))
	for i, f := range fixtures {
		obtained, err := toNSec(f)
		AssertIsNil(t, err)
		AssertDeepEquals(t, "", obtained, expected[i])
	}
}
func TestParseAppName_Valid(t *testing.T) {
	buff := []byte("su ")
	appName := "su"
	assertParseAppName(t, appName, buff, 2, nil)
}
func TestParseAppName_TooLong(t *testing.T) {
	// > 48chars
	buff := []byte("suuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu ")
	appName := ""
	assertParseAppName(t, appName, buff, 0, ErrInvalidAppName)
}
func TestParseProcId_Valid(t *testing.T) {
	buff := []byte("123foo ")
	procId := "123foo"
	assertParseProcId(t, procId, buff, 6, nil)
}
func TestParseProcId_TooLong(t *testing.T) {
	// > 128chars
	buff := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab ")
	procId := ""
	assertParseProcId(t, procId, buff, 0, ErrInvalidProcId)
}
func TestParseMsgId_Valid(t *testing.T) {
	buff := []byte("123foo ")
	procId := "123foo"
	assertParseMsgId(t, procId, buff, 6, nil)
}
func TestParseMsgId_TooLong(t *testing.T) {
	// > 32chars
	buff := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ")
	procId := ""
	assertParseMsgId(t, procId, buff, 0, ErrInvalidMsgId)
}
func TestParseStructuredData_NilValue(t *testing.T) {
	// > 32chars
	buff := []byte("-")
	sdData := "-"
	assertParseSdName(t, sdData, buff, 1, nil)
}
func TestParseStructuredData_SingleStructuredData(t *testing.T) {
	sdData := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"]`
	buff := []byte(sdData)
	assertParseSdName(t, sdData, buff, len(buff), nil)
}
func TestParseStructuredData_MultipleStructuredData(t *testing.T) {
	sdData := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"][examplePriority@32473 class="high"]`
	buff := []byte(sdData)
	assertParseSdName(t, sdData, buff, len(buff), nil)
}
func TestParseStructuredData_MultipleStructuredDataInvalid(t *testing.T) {
	a := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"]`
	sdData := a + ` [examplePriority@32473 class="high"]`
	buff := []byte(sdData)
	assertParseSdName(t, a, buff, len(a), nil)
}

// -------------
func BenchmarkParseTimestamp(t *testing.B) {
	buff := []byte("2003-08-24T05:14:15.000003-07:00")
	p := &rfc5424{}
	for i := 0; i < t.N; i++ {
		_, _, err := p.parseTimestamp(buff)
		if err != nil {
			panic(err)
		}
	}
}

// func  BenchmarkParseHeader(t *testing.T) {
// 	buff := []byte("<165>1 2003-10-11T22:14:15.003Z mymachine.example.com su 123 ID47")
// 	p := &rfc5424Parser{
// 		buff:   buff,
// 		cursor: 0,
// 		l:      len(buff),
// 	}
// 	for i := 0; i < c.N; i++ {
// 		_, err := p.parseHeader()
// 		if err != nil {
// 			panic(err)
// 		}
// 		p.cursor = 0
// 	}
// }
//////////////////////////////////
func assertRfc5424Timestamp(t *testing.T, ts time.Time, b []byte, excepted_cursor int, excepted_error error) {
	var p rfc5424
	cursor, obtained, err := p.parseTimestamp(b)
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
func assertParseYear(t *testing.T, year int, b []byte, expC int, e error) {
	cursor, obtained, err := parseYear(b)
	AssertDeepEquals(t, "", obtained, year)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseMonth(t *testing.T, month int, b []byte, expC int, e error) {
	cursor, obtained, err := parseMonth(b)
	AssertDeepEquals(t, "", obtained, month)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseDay(t *testing.T, day int, b []byte, expC int, e error) {
	cursor, obtained, err := parseDay(b)
	AssertDeepEquals(t, "", obtained, day)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseFullDate(t *testing.T, fd fullDate, b []byte, expC int, e error) {
	cursor, obtained, err := parseFullDate(b)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", obtained, fd)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseHour(t *testing.T, hour int, b []byte, expC int, e error) {
	cursor, obtained, err := parseHour(b)
	AssertDeepEquals(t, "", obtained, hour)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseMinute(t *testing.T, minute int, b []byte, expC int, e error) {
	cursor, obtained, err := parseMinute(b)
	AssertDeepEquals(t, "", obtained, minute)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseSecond(t *testing.T, second int, b []byte, expC int, e error) {
	cursor, obtained, err := parseSecond(b)
	AssertDeepEquals(t, "", obtained, second)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseSecFrac(t *testing.T, secFrac float64, b []byte, expC int, e error) {
	cursor, obtained, err := parseSecFrac(b)
	AssertDeepEquals(t, "", obtained, secFrac)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseAppName(t *testing.T, appName string, b []byte, expC int, e error) {
	p := &rfc5424{}
	cursor, obtained, err := p.parseAppName(b)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", obtained, appName)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseProcId(t *testing.T, procId string, b []byte, expC int, e error) {
	p := &rfc5424{}
	cursor, obtained, err := p.parseProcId(b)
	AssertDeepEquals(t, "", err, e)
	if err == nil {
		AssertDeepEquals(t, "", obtained, procId)
	}
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseMsgId(t *testing.T, msgId string, b []byte, expC int, e error) {
	p := &rfc5424{}
	cursor, obtained, err := p.parseMsgId(b)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", obtained, msgId)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
func assertParseSdName(t *testing.T, sdData string, b []byte, expC int, e error) {
	p := &rfc5424{}
	cursor, obtained, err := p.parseStructuredData(b)
	AssertDeepEquals(t, "", err, e)
	AssertDeepEquals(t, "", obtained, sdData)
	AssertDeepEquals(t, "", len(b)-len(cursor), expC)
}
