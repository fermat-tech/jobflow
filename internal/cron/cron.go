// Package cron parses a useful subset of the standard 5-field cron syntax and
// computes the next activation time for a schedule. It has no dependencies.
//
// Supported field syntax (minute hour day-of-month month day-of-week):
//
//	*            any value
//	5            a single value
//	1-5          an inclusive range
//	1,3,5        a list of values/ranges
//	*/15         a step over the whole range
//	1-30/5       a step over a range
//
// Day-of-week accepts 0-6 (Sunday=0) or 7 for Sunday as well. Month and
// day-of-week also accept three-letter names (jan, mon, ...).
//
// Convenience macros are also accepted:
//
//	@yearly / @annually   "0 0 1 1 *"
//	@monthly              "0 0 1 * *"
//	@weekly               "0 0 * * 0"
//	@daily / @midnight    "0 0 * * *"
//	@hourly               "0 * * * *"
//	@every <duration>     fixed interval, e.g. "@every 30s" or "@every 1h30m"
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron expression. The zero value is not usable; create
// one with Parse.
type Schedule struct {
	spec string

	// every, when non-zero, means this is an "@every <dur>" schedule and the
	// field bitsets below are unused.
	every time.Duration

	minute  uint64 // bits 0..59
	hour    uint64 // bits 0..23
	dom     uint64 // bits 1..31
	month   uint64 // bits 1..12
	dow     uint64 // bits 0..6 (Sunday=0)
	domStar bool   // day-of-month field was "*"
	dowStar bool   // day-of-week field was "*"
}

// String returns the original expression that produced the schedule.
func (s *Schedule) String() string { return s.spec }

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Parse compiles a cron expression. It returns an error describing the first
// problem encountered.
func Parse(spec string) (*Schedule, error) {
	trimmed := strings.TrimSpace(spec)
	if trimmed == "" {
		return nil, fmt.Errorf("cron: empty schedule")
	}

	if strings.HasPrefix(trimmed, "@") {
		return parseMacro(spec, trimmed)
	}

	fields := strings.Fields(trimmed)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), spec)
	}

	s := &Schedule{spec: spec}
	var err error
	if s.minute, _, err = parseField(fields[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron: minute field: %w", err)
	}
	if s.hour, _, err = parseField(fields[1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("cron: hour field: %w", err)
	}
	if s.dom, s.domStar, err = parseField(fields[2], 1, 31, nil); err != nil {
		return nil, fmt.Errorf("cron: day-of-month field: %w", err)
	}
	if s.month, _, err = parseField(fields[3], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("cron: month field: %w", err)
	}
	if s.dow, s.dowStar, err = parseDOW(fields[4]); err != nil {
		return nil, fmt.Errorf("cron: day-of-week field: %w", err)
	}
	return s, nil
}

func parseMacro(orig, trimmed string) (*Schedule, error) {
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "@every ") {
		durStr := strings.TrimSpace(trimmed[len("@every "):])
		d, err := time.ParseDuration(durStr)
		if err != nil {
			return nil, fmt.Errorf("cron: invalid @every duration %q: %w", durStr, err)
		}
		if d < time.Second {
			return nil, fmt.Errorf("cron: @every duration must be at least 1s, got %s", d)
		}
		return &Schedule{spec: orig, every: d}, nil
	}

	var expanded string
	switch lower {
	case "@yearly", "@annually":
		expanded = "0 0 1 1 *"
	case "@monthly":
		expanded = "0 0 1 * *"
	case "@weekly":
		expanded = "0 0 * * 0"
	case "@daily", "@midnight":
		expanded = "0 0 * * *"
	case "@hourly":
		expanded = "0 * * * *"
	default:
		return nil, fmt.Errorf("cron: unknown macro %q", trimmed)
	}
	s, err := Parse(expanded)
	if err != nil {
		return nil, err
	}
	s.spec = orig
	return s, nil
}

// parseDOW handles the day-of-week quirks: 7 is an alias for Sunday (0).
func parseDOW(field string) (bits uint64, star bool, err error) {
	bits, star, err = parseField(field, 0, 7, dowNames)
	if err != nil {
		return 0, false, err
	}
	// Fold 7 (Sunday alias) down onto 0.
	if bits&(1<<7) != 0 {
		bits |= 1 << 0
		bits &^= 1 << 7
	}
	return bits, star, nil
}

// parseField parses a single cron field into a bitset. names, when non-nil,
// maps lowercase three-letter aliases to their numeric value.
func parseField(field string, min, max int, names map[string]int) (bits uint64, star bool, err error) {
	if field == "*" {
		for i := min; i <= max; i++ {
			bits |= 1 << uint(i)
		}
		return bits, true, nil
	}

	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return 0, false, fmt.Errorf("empty term in %q", field)
		}

		rangePart := part
		step := 1
		if slash := strings.Index(part, "/"); slash >= 0 {
			rangePart = part[:slash]
			step, err = strconv.Atoi(part[slash+1:])
			if err != nil || step <= 0 {
				return 0, false, fmt.Errorf("invalid step in %q", part)
			}
		}

		var lo, hi int
		switch {
		case rangePart == "*":
			lo, hi = min, max
		case strings.Contains(rangePart, "-"):
			ends := strings.SplitN(rangePart, "-", 2)
			if lo, err = parseValue(ends[0], names); err != nil {
				return 0, false, err
			}
			if hi, err = parseValue(ends[1], names); err != nil {
				return 0, false, err
			}
		default:
			if lo, err = parseValue(rangePart, names); err != nil {
				return 0, false, err
			}
			// A bare value with a step (e.g. "5/10") ranges to max.
			if step > 1 {
				hi = max
			} else {
				hi = lo
			}
		}

		if lo < min || hi > max || lo > hi {
			return 0, false, fmt.Errorf("value out of range [%d,%d] in %q", min, max, part)
		}
		for v := lo; v <= hi; v += step {
			bits |= 1 << uint(v)
		}
	}
	return bits, false, nil
}

func parseValue(s string, names map[string]int) (int, error) {
	s = strings.TrimSpace(s)
	if names != nil {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	return v, nil
}

// Next returns the earliest time strictly after t at which the schedule fires.
// It returns the zero time if no activation occurs within ~5 years (which
// indicates an impossible schedule such as "0 0 30 2 *").
func (s *Schedule) Next(t time.Time) time.Time {
	if s.every != 0 {
		return t.Add(s.every)
	}

	// Start from the next whole minute.
	t = t.Add(time.Minute - time.Duration(t.Second())*time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)

	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		if s.month&(1<<uint(t.Month())) == 0 {
			// Jump to the first day of next month.
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, 1, 0)
			continue
		}
		if !s.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
			continue
		}
		if s.hour&(1<<uint(t.Hour())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Add(time.Hour)
			continue
		}
		if s.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

// dayMatches implements the standard cron rule: when both day-of-month and
// day-of-week are restricted (neither is "*"), a day matches if EITHER field
// matches. Otherwise the restricted field(s) must match.
func (s *Schedule) dayMatches(t time.Time) bool {
	domOK := s.dom&(1<<uint(t.Day())) != 0
	dowOK := s.dow&(1<<uint(t.Weekday())) != 0
	if !s.domStar && !s.dowStar {
		return domOK || dowOK
	}
	return domOK && dowOK
}
