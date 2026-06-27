package engine

import (
	"context"
	"sync"
)

// RunAll runs every registered job once and returns each job's run keyed by job
// name.
//
// When ordered is false, all jobs are launched immediately and concurrently,
// ignoring job dependencies (like Trigger). When ordered is true, jobs run in
// dependency order — a job starts only after all of its DependsOn jobs have
// succeeded in this invocation, and is recorded as skipped if any dependency
// failed or was itself skipped. Independent jobs still run concurrently. It is
// a one-shot of the scheduler's cascade, driven entirely by this run's results
// rather than persisted history.
func (e *Engine) RunAll(ctx context.Context, ordered bool) map[string]*Run {
	e.mu.Lock()
	names := append([]string(nil), e.order...)
	jobs := make(map[string]*Job, len(names))
	for _, n := range names {
		jobs[n] = e.jobs[n]
	}
	e.mu.Unlock()

	results := make(map[string]*Run, len(names))
	var mu sync.Mutex
	setResult := func(name string, r *Run) {
		mu.Lock()
		results[name] = r
		mu.Unlock()
	}

	if !ordered {
		var wg sync.WaitGroup
		for _, n := range names {
			if !e.tryStart(n) {
				continue // already running elsewhere (e.g. an active scheduler)
			}
			wg.Add(1)
			go func(j *Job) {
				defer wg.Done()
				defer e.finishStart(j.Name)
				setResult(j.Name, e.executeRun(ctx, j, TriggerManual, ""))
			}(jobs[n])
		}
		wg.Wait()
		return results
	}

	// Ordered: wave-based execution over the job dependency DAG. state is only
	// touched by this goroutine; workers report completions over ch.
	state := make(map[string]stepState, len(names))
	for _, n := range names {
		state[n] = stPending
	}
	depsPassed := func(n string) bool {
		for _, d := range jobs[n].DependsOn {
			if state[d] != stPassed {
				return false
			}
		}
		return true
	}
	depBlocked := func(n string) bool {
		for _, d := range jobs[n].DependsOn {
			if s := state[d]; s == stFailed || s == stBlocked {
				return true
			}
		}
		return false
	}

	type done struct {
		name string
		run  *Run
	}
	ch := make(chan done)
	running := 0

	for {
		// Fixpoint scan: launch every ready job and skip every dead one until a
		// pass makes no change (so blocking propagates regardless of order).
		for {
			changed := false
			for _, n := range names {
				if state[n] != stPending {
					continue
				}
				switch {
				case depBlocked(n) || ctx.Err() != nil:
					state[n] = stBlocked
					setResult(n, e.recordSkip(jobs[n], TriggerManual, "dependency did not succeed"))
					changed = true
				case depsPassed(n):
					if !e.tryStart(n) {
						state[n] = stBlocked
						setResult(n, e.recordSkip(jobs[n], TriggerManual, "job already running"))
						changed = true
						continue
					}
					state[n] = stRunning
					running++
					changed = true
					go func(j *Job) {
						defer e.finishStart(j.Name)
						ch <- done{j.Name, e.executeRun(ctx, j, TriggerManual, "")}
					}(jobs[n])
				}
			}
			if !changed {
				break
			}
		}
		if running == 0 {
			break
		}
		d := <-ch
		running--
		setResult(d.name, d.run)
		if d.run.Status == StatusSucceeded {
			state[d.name] = stPassed
		} else {
			state[d.name] = stFailed
		}
	}
	return results
}
