package service

import "testing"

func TestParseTime(t *testing.T) {
	tt := ParseTime("now()-24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = ParseTime("now() - 24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = ParseTime("now() -24h")
	if tt.IsZero() {
		t.Error(tt)
	}
	tt = ParseTime("now()- 24h")
	if tt.IsZero() {
		t.Error(tt)
	}
}
