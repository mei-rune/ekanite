package http

import "testing"

func TestParseTime(t *testing.T) {
	tt := parseTime("now()-24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = parseTime("now() - 24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = parseTime("now() -24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = parseTime("now()- 24h")
	if tt.IsZero() {
		t.Error(tt)
	}
}
