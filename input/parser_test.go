package input

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

/*
func Test_Formats(t *testing.T) {
	var p *Parser
	mismatched := func(rtrnd string, intnd string, intndA string) {
		if intndA != "" {
			t.Fatalf("Parser format %v does not match the intended format %v.\n", rtrnd, intnd)
		}
		t.Fatalf("Parser format %v does not match the intended format %v (same as: %v).\n", rtrnd, intndA, intnd)
	}
	for i, f := range fmtsByName {
		p, _ = NewParser(f)
		if p.fmt != fmtsByStandard[i] {
			mismatched(p.fmt, f, fmtsByStandard[i])
		}
	}
	for _, f := range fmtsByStandard {
		p, _ = NewParser(f)
		if p.fmt != f {
			mismatched(p.fmt, f, "")
		}
	}
	p, err := NewParser("unknown-format")
	if err == nil {
		t.Fatalf("parser successfully created with invalid format")
	}
}
*/

func Test_Parsing(t *testing.T) {
	now := time.Now()
	tests := []struct {
		fmt      string
		message  string
		expected map[string]interface{}
		fail     bool
	}{
		{
			fmt:     "rfc3164",
			message: `<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
			expected: map[string]interface{}{
				"timestamp": ToJavaTime(time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC)),
				"host":      "mymachine",
				"tag":       "very.large.syslog.message.tag",
				"message":   "'su root' failed for lonvick on /dev/pts/8",
				"priority":  34,
				"facility":  4,
				"severity":  2,
				"version":   NO_VERSION,
			}},
		{
			fmt:     "rfc3164",
			message: `<34>Oct 11 22:14:15 2016 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
			expected: map[string]interface{}{
				"timestamp": ToJavaTime(time.Date(2016, time.October, 11, 22, 14, 15, 0, time.UTC)),
				"host":      "mymachine",
				"tag":       "very.large.syslog.message.tag",
				"message":   "'su root' failed for lonvick on /dev/pts/8",
				"priority":  34,
				"facility":  4,
				"severity":  2,
				"version":   NO_VERSION,
			}},
		{
			fmt:     "rfc3164",
			message: `<34> asdfsadf`,
			expected: map[string]interface{}{
				"message":  "asdfsadf",
				"priority": 34,
				"facility": 4,
				"severity": 2,
				"version":  NO_VERSION,
			}},
		{
			fmt:     "rfc3164",
			message: `asdfsadf`,
			expected: map[string]interface{}{
				"message":  "asdfsadf",
				"priority": 0,
				"facility": 0,
				"severity": 0,
				"version":  NO_VERSION,
			}},
		{
			fmt: "syslog",
			// no STRUCTURED-DATA
			message: "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - 'su root' failed for lonvick on /dev/pts/8",
			expected: map[string]interface{}{
				"priority":        34,
				"facility":        4,
				"severity":        2,
				"version":         1,
				"timestamp":       ToJavaTime(time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)),
				"host":            "mymachine.example.com",
				"app":             "su",
				"pid":             -1,
				"message_id":      "ID47",
				"structured_data": "-",
				"message":         "'su root' failed for lonvick on /dev/pts/8",
			}},
		{
			fmt:     "syslog",
			message: `"<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts."`,
			expected: map[string]interface{}{
				"priority":        165,
				"facility":        20,
				"severity":        5,
				"version":         1,
				"timestamp":       ToJavaTime(time.Date(2003, time.August, 24, 5, 14, 15, 3*10e2, time.FixedZone("-07:00", -7*60*60))),
				"host":            "192.0.2.1",
				"app":             "myproc",
				"pid":             8710,
				"message_id":      "-",
				"structured_data": "-",
				"message":         "%% It's time to make the do-nuts.",
			}},
		{
			fmt: "syslog",
			// with STRUCTURED-DATA
			message: `<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry...`,
			expected: map[string]interface{}{
				"priority":        165,
				"facility":        20,
				"severity":        5,
				"version":         1,
				"timestamp":       ToJavaTime(time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)),
				"host":            "mymachine.example.com",
				"app":             "evntslog",
				"pid":             -1,
				"message_id":      "ID47",
				"structured_data": `[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"]`,
				"message":         "An application event log entry...",
			}},

		{
			fmt: "syslog",
			// STRUCTURED-DATA Only
			message: `<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource= "Application" eventID="1011"][examplePriority@32473 class="high"]`,
			expected: map[string]interface{}{
				"priority":        165,
				"facility":        20,
				"severity":        5,
				"version":         1,
				"timestamp":       ToJavaTime(time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)),
				"host":            "mymachine.example.com",
				"app":             "evntslog",
				"pid":             -1,
				"message_id":      "ID47",
				"structured_data": `[exampleSDID@32473 iut="3" eventSource= "Application" eventID="1011"][examplePriority@32473 class="high"]`,
				"message":         "",
			}},
		{
			fmt:     "syslog",
			message: `<134>1 2003-08-24T05:14:15.000003-07:00 ubuntu sshd 1999 - password accepted`,
			expected: map[string]interface{}{
				"priority":   134,
				"version":    1,
				"timestamp":  "2003-08-24T05:14:15.000003-07:00",
				"host":       "ubuntu",
				"app":        "sshd",
				"pid":        1999,
				"message_id": "-",
				"message":    "password accepted",
			},
		},
		{
			fmt:     "syslog",
			message: `<33>5 1985-04-12T23:20:50.52Z test.com cron 304 - password accepted`,
			expected: map[string]interface{}{
				"priority":   33,
				"version":    5,
				"timestamp":  "1985-04-12T23:20:50.52Z",
				"host":       "test.com",
				"app":        "cron",
				"pid":        304,
				"message_id": "-",
				"message":    "password accepted",
			},
		},
		{
			fmt:     "syslog",
			message: `<1>0 1985-04-12T19:20:50.52-04:00 test.com cron 65535 - password accepted`,
			expected: map[string]interface{}{
				"priority":   1,
				"version":    0,
				"timestamp":  "1985-04-12T19:20:50.52-04:00",
				"host":       "test.com",
				"app":        "cron",
				"pid":        65535,
				"message_id": "-",
				"message":    "password accepted",
			},
		},
		{
			fmt:     "syslog",
			message: `<1>0 2003-10-11T22:14:15.003Z test.com cron 65535 msgid1234 password accepted`,
			expected: map[string]interface{}{
				"priority":   1,
				"version":    0,
				"timestamp":  "2003-10-11T22:14:15.003Z",
				"host":       "test.com",
				"app":        "cron",
				"pid":        65535,
				"message_id": "msgid1234",
				"message":    "password accepted",
			},
		},
		{
			fmt:     "syslog",
			message: `<1>0 2003-08-24T05:14:15.000003-07:00 test.com cron 65535 - JVM NPE\nsome_file.java:48\n\tsome_other_file.java:902`,
			expected: map[string]interface{}{
				"priority":   1,
				"version":    0,
				"timestamp":  "2003-08-24T05:14:15.000003-07:00",
				"host":       "test.com",
				"app":        "cron",
				"pid":        65535,
				"message_id": "-",
				"message":    `JVM NPE\nsome_file.java:48\n\tsome_other_file.java:902`,
			},
		},
		{
			fmt:     "syslog",
			message: `<27>1 2015-03-02T22:53:45-08:00 localhost.localdomain puppet-agent 5334 - mirrorurls.extend(list(self.metalink_data.urls()))`,
			expected: map[string]interface{}{
				"priority":   27,
				"version":    1,
				"timestamp":  "2015-03-02T22:53:45-08:00",
				"host":       "localhost.localdomain",
				"app":        "puppet-agent",
				"pid":        5334,
				"message_id": "-",
				"message":    "mirrorurls.extend(list(self.metalink_data.urls()))",
			},
		},
		{
			fmt:     "syslog",
			message: `<29>1 2015-03-03T06:49:08-08:00 localhost.localdomain puppet-agent 51564 - (/Stage[main]/Users_prd/Ssh_authorized_key[1063-username]) Dependency Group[group] has failures: true`,
			expected: map[string]interface{}{
				"priority":   29,
				"version":    1,
				"timestamp":  "2015-03-03T06:49:08-08:00",
				"host":       "localhost.localdomain",
				"app":        "puppet-agent",
				"pid":        51564,
				"message_id": "-",
				"message":    "(/Stage[main]/Users_prd/Ssh_authorized_key[1063-username]) Dependency Group[group] has failures: true",
			},
		},
		{
			fmt:     "syslog",
			message: `<142>1 2015-03-02T22:23:07-08:00 localhost.localdomain Keepalived_vrrp 21125 - VRRP_Instance(VI_1) ignoring received advertisement...`,
			expected: map[string]interface{}{
				"priority":   142,
				"version":    1,
				"timestamp":  "2015-03-02T22:23:07-08:00",
				"host":       "localhost.localdomain",
				"app":        "Keepalived_vrrp",
				"pid":        21125,
				"message_id": "-",
				"message":    "VRRP_Instance(VI_1) ignoring received advertisement...",
			},
		},
		{
			fmt:     "syslog",
			message: `<142>1 2015-03-02T22:23:07-08:00 localhost.localdomain Keepalived_vrrp 21125 - HEAD /wp-login.php HTTP/1.1" 200 167 "http://www.philipotoole.com/" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.97 Safari/537.11`,
			expected: map[string]interface{}{
				"priority":   142,
				"version":    1,
				"timestamp":  "2015-03-02T22:23:07-08:00",
				"host":       "localhost.localdomain",
				"app":        "Keepalived_vrrp",
				"pid":        21125,
				"message_id": "-",
				"message":    `HEAD /wp-login.php HTTP/1.1" 200 167 "http://www.philipotoole.com/" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.97 Safari/537.11`,
			},
		},
		{
			fmt:     "syslog",
			message: `<134>0 2015-05-05T21:20:00.493320+00:00 fisher apache-access - - 173.247.206.174 - - [05/May/2015:21:19:52 +0000] "GET /2013/11/ HTTP/1.1" 200 22056 "http://www.philipotoole.com/" "Wget/1.15 (linux-gnu)"`,
			expected: map[string]interface{}{
				"priority":   134,
				"version":    0,
				"timestamp":  "2015-05-05T21:20:00.49332Z",
				"host":       "fisher",
				"app":        "apache-access",
				"pid":        -1,
				"message_id": "-",
				"message":    `173.247.206.174 - - [05/May/2015:21:19:52 +0000] "GET /2013/11/ HTTP/1.1" 200 22056 "http://www.philipotoole.com/" "Wget/1.15 (linux-gnu)"`,
			},
		},
		{
			fmt:     "syslog",
			message: `<134>0 2017-06-04T14:09:13+02:00 192.168.1.217 filterlog - - 67,,,0,vtnet0,match,pass,out,4,0x0,,127,3328,0,DF,6,tcp,366,192.168.1.66,31.13.86.4,50800,443,326,PA,1912507082:1912507408,2077294259,257,,`,
			expected: map[string]interface{}{
				"priority":   134,
				"version":    0,
				"timestamp":  "2017-06-04T14:09:13+02:00",
				"host":       "192.168.1.217",
				"app":        "filterlog",
				"pid":        0,
				"message_id": "-",
				"message":    `67,,,0,vtnet0,match,pass,out,4,0x0,,127,3328,0,DF,6,tcp,366,192.168.1.66,31.13.86.4,50800,443,326,PA,1912507082:1912507408,2077294259,257,,`,
			},
		},
		{
			fmt:     "syslog",
			message: `<134> 2013-09-04T10:25:52.618085 ubuntu sshd 1999 - password accepted`,
			expected: map[string]interface{}{
				"priority":  134,
				"version":   NO_VERSION,
				"timestamp": "2013-09-04T10:25:52.618085+08:00",
				"host":      "ubuntu",
				"app":       "sshd",
				"pid":       1999,
				"message":   `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33> 7 2013-09-04T10:25:52.618085 test.com cron 304 - password accepted`,
			expected: map[string]interface{}{
				"priority":  33,
				"version":   7,
				"timestamp": "2013-09-04T10:25:52.618085+08:00",
				"host":      "test.com",
				"app":       "cron",
				"pid":       304,
				"message":   `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33> 7 2013-09-04T10:25:52.618085 test.com cron 304 $ password accepted`,
			expected: map[string]interface{}{
				"priority":  33,
				"version":   7,
				"timestamp": "2013-09-04T10:25:52.618085+08:00",
				"host":      "test.com",
				"app":       "cron",
				"pid":       304,
				"message":   `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33> 7 2013-09-04T10:25:52.618085 test.com cron 304 - - password accepted`,
			expected: map[string]interface{}{
				"priority":  33,
				"version":   7,
				"timestamp": "2013-09-04T10:25:52.618085+08:00",
				"host":      "test.com",
				"app":       "cron",
				"pid":       304,
				"message":   `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33>7 2013-09-04T10:25:52.618085 test.com cron not_a_pid - password accepted`,
			expected: map[string]interface{}{
				"priority":  33,
				"version":   7,
				"timestamp": "2013-09-04T10:25:52.618085+08:00",
				"host":      "test.com",
				"app":       "cron",
				"pid":       "not_a_pid",
				"message":   `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33> password accepted`,
			expected: map[string]interface{}{
				"priority": 33,
				"version":  NO_VERSION,
				"message":  `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `<33>password accepted`,
			expected: map[string]interface{}{
				"priority": 33,
				"version":  NO_VERSION,
				"message":  `password accepted`,
			},
		},
		{
			fmt:     "syslog",
			message: `5:52.618085 test.com cron 65535 - password accepted`,
			expected: map[string]interface{}{
				"priority": 0,
				"version":  NO_VERSION,
				"message":  `5:52.618085 test.com cron 65535 - password accepted`,
			},
		},
	}

	for i, tt := range tests {
		p := CreateParser(tt.fmt)
		t.Logf("using %d\n", i+1)
		// fmt.Printf("using %d\n", i+1)
		result, err := p.Parse(bytes.NewBufferString(tt.message).Bytes())
		if tt.fail {
			if err == nil {
				t.Error("\n\nParser should fail.\n")
			}
		} else {
			if err != nil {
				t.Error("\n\nParser should succeed.\n", err)
			}
		}

		AssertDeepEquals(t, "", result, tt.expected)
	}
}
func Benchmark_Parsing(b *testing.B) {
	p := CreateParser("syslog")
	for n := 0; n < b.N; n++ {
		_, err := p.Parse(bytes.NewBufferString(`<134>0 2015-05-05T21:20:00.493320+00:00 fisher apache-access - - 173.247.206.174 - - [05/May/2015:21:19:52 +0000] "GET /2013/11/ HTTP/1.  1" 200 22056 "http://www.philipotoole.com/" "Wget/1.15 (linux-gnu)"`).Bytes())
		if err != nil {
			panic("message failed to parse during benchmarking")
		}
	}
}
func TestParsePriority_Empty(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("")
	assertPriority(t, pri, buff, 0, ErrPriorityEmpty)
}
func TestParsePriority_NoStart(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("7>")
	assertPriority(t, pri, buff, 0, ErrPriorityNoStart)
}
func TestParsePriority_NoEnd(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<77")
	assertPriority(t, pri, buff, 0, ErrPriorityNoEnd)
}
func TestParsePriority_TooShort(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<>")
	assertPriority(t, pri, buff, 0, ErrPriorityTooShort)
}
func TestParsePriority_TooLong(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<1233>")
	assertPriority(t, pri, buff, 0, ErrPriorityTooLong)
}
func TestParsePriority_NoDigits(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<7a8>")
	assertPriority(t, pri, buff, 0, ErrPriorityNonDigit)
}
func TestParsePriority_Ok(t *testing.T) {
	pri := newPriority(190)
	buff := []byte("<190>")
	assertPriority(t, pri, buff, 5, nil)
}
func TestNewPriority(t *testing.T) {
	obtained := newPriority(165)
	expected := Priority{
		P: 165,
		F: Facility{Value: 20},
		S: Severity{Value: 5},
	}
	AssertDeepEquals(t, "", obtained, expected)
}
func TestParseVersion_NotFound(t *testing.T) {
	buff := []byte("<123>")
	assertVersion(t, NO_VERSION, buff[5:], 0, ErrVersionNotFound)
}
func TestParseVersion_NonDigit(t *testing.T) {
	buff := []byte("<123>a")
	assertVersion(t, NO_VERSION, buff[5:], 0, ErrVersionNotFound)
}
func TestParseVersion_Ok(t *testing.T) {
	buff := []byte("<123>1")
	assertVersion(t, 1, buff[5:], 1, nil)
}
func TestParseHostname_Invalid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte(" foo name")
	hostname := "foo"
	assertHostname(t, hostname, buff)
}
func TestParseHostname_Valid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	hostname := "ubuntu11.somehost.com"
	buff := []byte(hostname + " ")
	assertHostname(t, hostname, buff)
}
func BenchmarkParsePriority(t *testing.B) {
	buff := []byte("<190>")
	for i := 0; i < t.N; i++ {
		_, _, err := ParsePriority(buff)
		if err != nil {
			panic(err)
		}
	}
}
func BenchmarkParseVersion(t *testing.B) {
	buff := []byte("<123>1")
	for i := 0; i < t.N; i++ {
		_, _, err := ParseVersion(buff)
		if err != nil {
			panic(err)
		}
	}
}
func assertPriority(t *testing.T, p Priority, b []byte, expC int, e error) {
	cursor, obtained, err := ParsePriority(b)
	if nil != err {
		if e.Error() != err.Error() {
			t.Errorf("excepted is %#v", e.Error())
			t.Errorf("actual is %#v", err.Error())
		}
		return
	}
	if len(b)-len(cursor) != expC {
		t.Errorf("excepted is %#v", expC)
		t.Errorf("actual is %#v", len(cursor))
		return
	}
	AssertDeepEquals(t, "", obtained, p)
}
func assertVersion(t *testing.T, version int, b []byte, expC int, e error) {
	cursor, obtained, err := ParseVersion(b)
	if nil != err {
		if nil == e {
			t.Errorf("excepted is nil")
			t.Errorf("actual is %#v", err.Error())
			return
		}
		if e.Error() != err.Error() {
			t.Errorf("excepted is %#v", e.Error())
			t.Errorf("actual is %#v", err.Error())
		}
		return
	}
	if len(b)-len(cursor) != expC {
		t.Errorf("excepted is %#v", expC)
		t.Errorf("actual is %#v", len(cursor))
		return
	}
	AssertDeepEquals(t, "", obtained, version)
}
func assertHostname(t *testing.T, h string, b []byte) {
	_, obtained := ParseHostname(b)
	AssertDeepEquals(t, "", obtained, h)
}

//func TestRfc3164TestSuite(t *testing.T) {
//	TestingT(t)
//}
func AssertDeepEquals(t *testing.T, key string, actual, excepted interface{}) {
	if m1, ok := actual.(map[string]interface{}); ok {
		if m2, ok := excepted.(map[string]interface{}); ok {
			// if len(m1) != len(m2) {
			// 	t.Errorf("[%v] excepted is %#v", key, excepted)
			// 	t.Errorf("[%v] actual is %#v", key, actual)
			// 	for k, _ := range m1 {
			// 		if _, ok := m2[k]; !ok {
			// 			t.Error("left has ", k)
			// 		}
			// 	}
			// 	for k, _ := range m2 {
			// 		if _, ok := m1[k]; !ok {
			// 			t.Error("right has ", k)
			// 		}
			// 	}
			// 	return
			// }
			for k, v := range m2 {
				AssertDeepEquals(t, key+"."+k, m1[k], v)
			}
			return
		}
		t.Errorf("[%v] excepted is %#v", key, excepted)
		t.Errorf("[%v] actual is %#v", key, actual)
		return
	}

	if t1, ok := actual.(time.Time); ok {
		if t2, ok := excepted.(time.Time); ok {
			if !t1.Equal(t2) {
				t.Errorf("[%v] actual is %#v", key, t1.Format(time.RFC3339Nano))
				t.Errorf("[%v] excepted is %#v", key, t2.Format(time.RFC3339Nano))
			}
			return
		}
	}

	if !reflect.DeepEqual(actual, excepted) {
		if t1, ok := actual.(time.Time); ok {
			t.Errorf("[%v] actual is %#v", key, t1.Format(time.RFC3339Nano))
		} else {
			t.Errorf("[%v] actual is %#v", key, actual)
		}
		if t2, ok := excepted.(time.Time); ok {
			t.Errorf("[%v] excepted is %#v", key, t2.Format(time.RFC3339Nano))
		} else {
			t.Errorf("[%v] excepted is %#v", key, excepted)
		}
	}
}
func AssertIsNil(t *testing.T, actual interface{}) {
	if nil != actual {
		t.Errorf("actual isn't nil, value is %#v", actual)
	}
}
func assertTag(t *testing.T, b []byte, excepted_cursor int, excepted_value interface{}) {
	cursor, obtained := ParseTag(b)
	if len(b)-len(cursor) != excepted_cursor {
		t.Errorf("excepted is %#v", excepted_cursor)
		t.Errorf("actual is %#v", len(cursor))
		return
	}
	AssertDeepEquals(t, "", obtained, excepted_value)
}
