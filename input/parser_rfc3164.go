package input

import (
	"bytes"
	"time"
	"unicode"
)

type rfc3164 struct {
	year string
}

func (self *rfc3164) Parse(bs []byte) (map[string]interface{}, error) {
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
	next, ts, _ := ParseTimestamp(next)
	var hostname, tag string
	if !ts.IsZero() {
		next = bytes.TrimRightFunc(next, unicode.IsSpace)
		old_next := next
		next, hostname = ParseHostname(next)
		if hostname == self.year { // 如果主机名 == 年，那么一定错了。
			hostname = ""
			next = old_next
		} else {
			next = bytes.TrimRightFunc(next, unicode.IsSpace)
			next, tag = ParseTag(next)
		}
	}
	content := bytes.TrimSpace(next)
	result := map[string]interface{}{
		"priority": pri.P,
		"facility": pri.F.Value,
		"severity": pri.S.Value,
		"version":  NO_VERSION,
		"message":  string(content),
	}
	if "" != hostname {
		result["host"] = hostname
	}
	if "" != tag {
		result["tag"] = tag
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	result["timestamp"] = ToJavaTime(ts)
	return result, nil
}
