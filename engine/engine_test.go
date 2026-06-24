package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestEngine returns an engine with a quiet logger and in-memory store.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return New(Options{
		Store:  NewMemoryStore(),
		Logger: log.New(io.Discard, "", 0),
	})
}

func TestRunAllStepsSucceed(t *testing.T) {
	eng := newTestEngine(t)
	var order []string
	var mu sync.Mutex
	rec := func(name string) HandlerFunc {
		return func(ctx context.Context, s Step) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	eng.Register("a", rec("a"))
	eng.Register("b", rec("b"))
	if err := eng.AddJob(&Job{Name: "job", Steps: []Step{
		{Name: "s1", Handler: "a"},
		{Name: "s2", Handler: "b"},
	}}); err != nil {
		t.Fatal(err)
	}

	run, err := eng.Trigger(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", run.Status)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("step order = %v, want [a b]", order)
	}
}

func TestStepFailureStopsJob(t *testing.T) {
	eng := newTestEngine(t)
	ran := map[string]bool{}
	var mu sync.Mutex
	mark := func(name string, fail bool) HandlerFunc {
		return func(ctx context.Context, s Step) error {
			mu.Lock()
			ran[name] = true
			mu.Unlock()
			if fail {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
	}
	eng.Register("ok", mark("ok", false))
	eng.Register("boom", mark("boom", true))
	eng.Register("after", mark("after", false))
	eng.AddJob(&Job{Name: "job", Steps: []Step{
		{Name: "s1", Handler: "ok"},
		{Name: "s2", Handler: "boom"},
		{Name: "s3", Handler: "after"},
	}})

	run, _ := eng.Trigger(context.Background(), "job")
	if run.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	if run.Steps[0].Status != StatusSucceeded || run.Steps[1].Status != StatusFailed || run.Steps[2].Status != StatusSkipped {
		t.Fatalf("step statuses = %s/%s/%s", run.Steps[0].Status, run.Steps[1].Status, run.Steps[2].Status)
	}
	if ran["after"] {
		t.Fatal("step after a failure should not have run")
	}
}

func TestContinueOnError(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("boom", func(ctx context.Context, s Step) error { return io.ErrUnexpectedEOF })
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	eng.AddJob(&Job{Name: "job", Steps: []Step{
		{Name: "s1", Handler: "boom", ContinueOnError: true},
		{Name: "s2", Handler: "noop"},
	}})

	run, _ := eng.Trigger(context.Background(), "job")
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (continueOnError)", run.Status)
	}
	if run.Steps[0].Status != StatusFailed || run.Steps[1].Status != StatusSucceeded {
		t.Fatalf("step statuses = %s/%s", run.Steps[0].Status, run.Steps[1].Status)
	}
}

func TestRetries(t *testing.T) {
	eng := newTestEngine(t)
	var attempts int32
	eng.Register("flaky", func(ctx context.Context, s Step) error {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	eng.AddJob(&Job{Name: "job", Steps: []Step{
		{Name: "s1", Handler: "flaky", Retries: 3},
	}})

	run, _ := eng.Trigger(context.Background(), "job")
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s, want succeeded after retries", run.Status)
	}
	if run.Steps[0].Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", run.Steps[0].Attempts)
	}
}

func TestRestartFromStep(t *testing.T) {
	eng := newTestEngine(t)
	ran := map[string]bool{}
	var mu sync.Mutex
	mk := func(name string) HandlerFunc {
		return func(ctx context.Context, s Step) error {
			mu.Lock()
			ran[name] = true
			mu.Unlock()
			return nil
		}
	}
	eng.Register("h1", mk("s1"))
	eng.Register("h2", mk("s2"))
	eng.Register("h3", mk("s3"))
	eng.AddJob(&Job{Name: "job", Steps: []Step{
		{Name: "s1", Handler: "h1"},
		{Name: "s2", Handler: "h2"},
		{Name: "s3", Handler: "h3"},
	}})

	run, err := eng.Restart(context.Background(), "job", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s", run.Status)
	}
	if run.Steps[0].Status != StatusSkipped {
		t.Fatalf("s1 status = %s, want skipped", run.Steps[0].Status)
	}
	if ran["s1"] {
		t.Fatal("s1 should have been skipped on restart from s2")
	}
	if !ran["s2"] || !ran["s3"] {
		t.Fatal("s2 and s3 should have run")
	}

	// Restart by 1-based index should resolve to the same step name.
	name, err := eng.resolveStep("job", "2")
	if err != nil || name != "s2" {
		t.Fatalf("resolveStep index: got %q, %v", name, err)
	}
}

func TestDependencyGatingSkips(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	eng.AddJob(&Job{Name: "a", Steps: []Step{{Name: "s", Handler: "noop"}}})
	eng.AddJob(&Job{Name: "c", DependsOn: []string{"a"}, Steps: []Step{{Name: "s", Handler: "noop"}}})

	// a has not run yet, so a cron-style launch of c (gating on) must skip.
	eng.launchAsync(context.Background(), eng.jobs["c"], TriggerCron, "", false)
	eng.wg.Wait()

	latest, _ := eng.Latest("c")
	if latest == nil || latest.Status != StatusSkipped {
		t.Fatalf("c should be skipped when dependency unsatisfied, got %v", latest)
	}
}

func TestDependencyCascade(t *testing.T) {
	eng := newTestEngine(t)
	var cRan int32
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	eng.Register("markC", func(ctx context.Context, s Step) error {
		atomic.AddInt32(&cRan, 1)
		return nil
	})
	eng.AddJob(&Job{Name: "a", Steps: []Step{{Name: "s", Handler: "noop"}}})
	eng.AddJob(&Job{Name: "b", Steps: []Step{{Name: "s", Handler: "noop"}}})
	eng.AddJob(&Job{Name: "c", DependsOn: []string{"a", "b"}, Steps: []Step{{Name: "s", Handler: "markC"}}})

	// Completing A alone must not trigger C (B still pending).
	eng.launchAsync(context.Background(), eng.jobs["a"], TriggerManual, "", true)
	eng.wg.Wait()
	if atomic.LoadInt32(&cRan) != 0 {
		t.Fatal("C ran before B succeeded")
	}

	// Completing B satisfies all of C's deps; cascade should run C exactly once.
	eng.launchAsync(context.Background(), eng.jobs["b"], TriggerManual, "", true)
	eng.wg.Wait()
	if got := atomic.LoadInt32(&cRan); got != 1 {
		t.Fatalf("C ran %d times, want 1", got)
	}

	latest, _ := eng.Latest("c")
	if latest == nil || latest.Status != StatusSucceeded || latest.Trigger != TriggerDependency {
		t.Fatalf("C latest = %+v, want succeeded via dependency", latest)
	}
}

func TestCycleDetection(t *testing.T) {
	// A two-node cycle x <-> y must be rejected by validateGraph.
	jobs := map[string]*Job{
		"x": {Name: "x", DependsOn: []string{"y"}, Steps: []Step{{Name: "s", Handler: "noop"}}},
		"y": {Name: "y", DependsOn: []string{"x"}, Steps: []Step{{Name: "s", Handler: "noop"}}},
	}
	if err := validateGraph(jobs); err == nil {
		t.Fatal("expected cycle detection error")
	}

	// A valid linear chain a -> b must be accepted incrementally by AddJob.
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	if err := eng.AddJob(&Job{Name: "a", Steps: []Step{{Name: "s", Handler: "noop"}}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.AddJob(&Job{Name: "b", DependsOn: []string{"a"}, Steps: []Step{{Name: "s", Handler: "noop"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestLogTimestampFixedWidth(t *testing.T) {
	var buf bytes.Buffer
	// 459000 ns -> ".000459000" (zero-filled to 9 digits, not trimmed).
	fixed := time.Date(2026, 6, 24, 13, 17, 0, 459000, time.FixedZone("CDT", -5*3600))
	eng := New(Options{
		Logger: log.New(&buf, "", 0),
		Now:    func() time.Time { return fixed },
	})
	eng.logf("job %q already running; skipping cron trigger", "sleep")

	want := "2026-06-24T13:17:00.000459000-05:00 job \"sleep\" already running; skipping cron trigger\n"
	if got := buf.String(); got != want {
		t.Fatalf("log line mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestShellMissingFlagWarning(t *testing.T) {
	cases := []struct {
		name     string
		shell    []string
		suppress []string
		wantWarn bool
	}{
		{"bash no flag", []string{"/usr/bin/bash"}, nil, true},
		{"bash with -c", []string{"/usr/bin/bash", "-c"}, nil, false},
		{"bash.exe no flag", []string{`C:\tools\bash.exe`}, nil, true},
		{"cmd no flag", []string{"cmd"}, nil, true},
		{"powershell single is fine", []string{"powershell.exe"}, nil, false},
		{"unknown shell single is fine", []string{"myshell"}, nil, false},
		{"default two-element", nil, nil, false},
		{"suppress all", []string{"bash"}, []string{"all"}, false},
		{"suppress by code", []string{"bash"}, []string{"shell-missing-flag"}, false},
		{"suppress unrelated code", []string{"bash"}, []string{"other-code"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			New(Options{
				Logger:           log.New(&buf, "", 0),
				Shell:            c.shell,
				SuppressWarnings: c.suppress,
				Now:              func() time.Time { return time.Unix(0, 0).UTC() },
			})
			got := strings.Contains(buf.String(), string(WarnShellMissingFlag))
			if got != c.wantWarn {
				t.Fatalf("warn=%v, want %v; output=%q", got, c.wantWarn, buf.String())
			}
		})
	}
}

func TestUnknownDependencyRejected(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	if err := eng.AddJob(&Job{Name: "c", DependsOn: []string{"missing"}, Steps: []Step{{Name: "s", Handler: "noop"}}}); err == nil {
		t.Fatal("expected error for dependency on unknown job")
	}
}

// TestParallelStepsRunConcurrently proves that two steps sharing a single
// upstream dependency actually overlap: each waits on a barrier that only
// releases once both have started. Sequential execution would deadlock and the
// per-step timeout would fail the run.
func TestParallelStepsRunConcurrently(t *testing.T) {
	eng := newTestEngine(t)
	var wg sync.WaitGroup
	wg.Add(2)
	barrier := func(ctx context.Context, s Step) error {
		wg.Done()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			return nil
		case <-time.After(2 * time.Second):
			return fmt.Errorf("step %q never saw its parallel peer start", s.Name)
		}
	}
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	eng.Register("barrier", barrier)
	if err := eng.AddJob(&Job{Name: "j", Steps: []Step{
		{Name: "a", Handler: "noop"},
		{Name: "b", Handler: "barrier", DependsOn: []string{"a"}},
		{Name: "c", Handler: "barrier", DependsOn: []string{"a"}},
	}}); err != nil {
		t.Fatal(err)
	}

	run, err := eng.Trigger(context.Background(), "j")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s; parallel steps likely ran sequentially", run.Status)
	}
}

// TestStepFanInAndBlocking covers a diamond a -> {b, c} -> d where c fails:
// b still runs, c fails, d is blocked (a dependency did not succeed), and the
// job fails.
func TestStepFanInAndBlocking(t *testing.T) {
	eng := newTestEngine(t)
	var dRan int32
	eng.Register("ok", func(ctx context.Context, s Step) error { return nil })
	eng.Register("boom", func(ctx context.Context, s Step) error { return io.ErrUnexpectedEOF })
	eng.Register("markD", func(ctx context.Context, s Step) error { atomic.AddInt32(&dRan, 1); return nil })
	if err := eng.AddJob(&Job{Name: "j", Steps: []Step{
		{Name: "a", Handler: "ok"},
		{Name: "b", Handler: "ok", DependsOn: []string{"a"}},
		{Name: "c", Handler: "boom", DependsOn: []string{"a"}},
		{Name: "d", Handler: "markD", DependsOn: []string{"b", "c"}},
	}}); err != nil {
		t.Fatal(err)
	}

	run, _ := eng.Trigger(context.Background(), "j")
	if run.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	got := map[string]Status{}
	for _, s := range run.Steps {
		got[s.Name] = s.Status
	}
	if got["a"] != StatusSucceeded || got["b"] != StatusSucceeded {
		t.Fatalf("a/b = %s/%s, want succeeded", got["a"], got["b"])
	}
	if got["c"] != StatusFailed {
		t.Fatalf("c = %s, want failed", got["c"])
	}
	if got["d"] != StatusSkipped {
		t.Fatalf("d = %s, want skipped (blocked)", got["d"])
	}
	if atomic.LoadInt32(&dRan) != 0 {
		t.Fatal("d should not have executed when c failed")
	}
}

// TestRestartRerunsOnlyDependents verifies restart-from-step over a DAG:
// restarting a -> {b, c} from "b" re-runs only b (c does not depend on b).
func TestRestartRerunsOnlyDependents(t *testing.T) {
	eng := newTestEngine(t)
	ran := map[string]int{}
	var mu sync.Mutex
	mk := func() HandlerFunc {
		return func(ctx context.Context, s Step) error {
			mu.Lock()
			ran[s.Name]++
			mu.Unlock()
			return nil
		}
	}
	eng.Register("h", mk())
	if err := eng.AddJob(&Job{Name: "j", Steps: []Step{
		{Name: "a", Handler: "h"},
		{Name: "b", Handler: "h", DependsOn: []string{"a"}},
		{Name: "c", Handler: "h", DependsOn: []string{"a"}},
	}}); err != nil {
		t.Fatal(err)
	}

	run, err := eng.Restart(context.Background(), "j", "b")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusSucceeded {
		t.Fatalf("status = %s", run.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if ran["a"] != 0 || ran["c"] != 0 {
		t.Fatalf("a/c should be presumed done on restart-from-b, ran a=%d c=%d", ran["a"], ran["c"])
	}
	if ran["b"] != 1 {
		t.Fatalf("b should have run once, ran %d", ran["b"])
	}
}

func TestStepCycleRejected(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	if err := eng.AddJob(&Job{Name: "j", Steps: []Step{
		{Name: "a", Handler: "noop", DependsOn: []string{"b"}},
		{Name: "b", Handler: "noop", DependsOn: []string{"a"}},
	}}); err == nil {
		t.Fatal("expected error for step dependency cycle")
	}
	if err := eng.AddJob(&Job{Name: "k", Steps: []Step{
		{Name: "a", Handler: "noop", DependsOn: []string{"ghost"}},
	}}); err == nil {
		t.Fatal("expected error for step depending on unknown step")
	}
}
