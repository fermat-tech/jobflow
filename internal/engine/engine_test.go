package engine

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
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

	// Restart by 1-based index should match restart by name.
	idx, err := eng.resolveStep("job", "2")
	if err != nil || idx != 1 {
		t.Fatalf("resolveStep index: got %d, %v", idx, err)
	}
}

func TestDependencyGatingSkips(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	eng.AddJob(&Job{Name: "a", Steps: []Step{{Name: "s", Handler: "noop"}}})
	eng.AddJob(&Job{Name: "c", DependsOn: []string{"a"}, Steps: []Step{{Name: "s", Handler: "noop"}}})

	// a has not run yet, so a cron-style launch of c (gating on) must skip.
	eng.launchAsync(context.Background(), eng.jobs["c"], TriggerCron, 0, false)
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
	eng.launchAsync(context.Background(), eng.jobs["a"], TriggerManual, 0, true)
	eng.wg.Wait()
	if atomic.LoadInt32(&cRan) != 0 {
		t.Fatal("C ran before B succeeded")
	}

	// Completing B satisfies all of C's deps; cascade should run C exactly once.
	eng.launchAsync(context.Background(), eng.jobs["b"], TriggerManual, 0, true)
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

func TestUnknownDependencyRejected(t *testing.T) {
	eng := newTestEngine(t)
	eng.Register("noop", func(ctx context.Context, s Step) error { return nil })
	if err := eng.AddJob(&Job{Name: "c", DependsOn: []string{"missing"}, Steps: []Step{{Name: "s", Handler: "noop"}}}); err == nil {
		t.Fatal("expected error for dependency on unknown job")
	}
}
