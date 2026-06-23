package cron

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, spec string) *Schedule {
	t.Helper()
	s, err := Parse(spec)
	if err != nil {
		t.Fatalf("Parse(%q): %v", spec, err)
	}
	return s
}

func TestNext(t *testing.T) {
	base := time.Date(2026, 6, 23, 10, 30, 15, 0, time.UTC)
	cases := []struct {
		spec string
		want time.Time
	}{
		{"* * * * *", time.Date(2026, 6, 23, 10, 31, 0, 0, time.UTC)},
		{"*/15 * * * *", time.Date(2026, 6, 23, 10, 45, 0, 0, time.UTC)},
		{"0 * * * *", time.Date(2026, 6, 23, 11, 0, 0, 0, time.UTC)},
		{"@hourly", time.Date(2026, 6, 23, 11, 0, 0, 0, time.UTC)},
		{"@daily", time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC)},
		{"30 9 * * *", time.Date(2026, 6, 24, 9, 30, 0, 0, time.UTC)},
		{"0 0 1 * *", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
		// June 23 2026 is a Tuesday; next Monday is June 29.
		{"0 0 * * mon", time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC)},
		{"0 0 * * 1", time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		got := mustParse(t, c.spec).Next(base)
		if !got.Equal(c.want) {
			t.Errorf("%q: Next = %v, want %v", c.spec, got, c.want)
		}
	}
}

func TestEvery(t *testing.T) {
	base := time.Date(2026, 6, 23, 10, 30, 15, 0, time.UTC)
	got := mustParse(t, "@every 90s").Next(base)
	want := base.Add(90 * time.Second)
	if !got.Equal(want) {
		t.Errorf("@every 90s: Next = %v, want %v", got, want)
	}
}

func TestDOMorDOW(t *testing.T) {
	// When both DOM and DOW are restricted, either matching fires the job.
	// "0 0 13 * fri" -> midnight on the 13th OR any Friday.
	s := mustParse(t, "0 0 13 * fri")
	// From Jun 23 2026 (Tue), the next Friday is Jun 26.
	base := time.Date(2026, 6, 23, 10, 0, 0, 0, time.UTC)
	got := s.Next(base)
	want := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("DOM-or-DOW: Next = %v, want %v", got, want)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{"", "* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "@every 1ms", "@bogus", "1-x * * * *"}
	for _, spec := range bad {
		if _, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q): expected error, got nil", spec)
		}
	}
}
