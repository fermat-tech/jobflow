package engine

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/fermat-tech/jobflow/cron"
)

// Options configures a new Engine. The zero value is valid; sensible defaults
// are filled in by New.
type Options struct {
	// Store persists run state. Defaults to an in-memory store.
	Store Store
	// Shell is the command prefix used to run Command steps. Defaults to
	// {"cmd", "/C"} on Windows and {"/bin/sh", "-c"} elsewhere.
	Shell []string
	// Logger receives scheduler activity. The engine prepends its own
	// RFC3339Nano timestamp to each line, so a custom logger should be
	// flag-less (log.New(w, "", 0)). Defaults to a flag-less stderr logger.
	Logger *log.Logger
	// Stdout/Stderr receive Command step output. Default to os.Stdout/Stderr.
	Stdout, Stderr io.Writer
	// Now is the clock, injectable for tests. Defaults to time.Now.
	Now func() time.Time
}

// Engine schedules and runs jobs. It is safe for concurrent use; Trigger and
// Restart may be called while Run's scheduling loop is active.
type Engine struct {
	registry *Registry
	store    Store
	shell    []string
	logger   *log.Logger
	stdout   io.Writer
	stderr   io.Writer
	now      func() time.Time

	mu       sync.Mutex
	jobs     map[string]*Job
	order    []string             // job insertion order, for stable listing
	latest   map[string]*Run      // latest run per job
	running  map[string]bool      // jobs currently executing
	nextFire map[string]time.Time // next scheduled fire per scheduled job
	started  bool

	wake chan struct{}  // signals the Run loop to recompute its sleep deadline
	wg   sync.WaitGroup // tracks in-flight async runs
}

// New creates an Engine with the given options.
func New(opts Options) *Engine {
	e := &Engine{
		registry: NewRegistry(),
		store:    opts.Store,
		shell:    opts.Shell,
		logger:   opts.Logger,
		stdout:   opts.Stdout,
		stderr:   opts.Stderr,
		now:      opts.Now,
		jobs:     make(map[string]*Job),
		latest:   make(map[string]*Run),
		running:  make(map[string]bool),
		nextFire: make(map[string]time.Time),
		wake:     make(chan struct{}, 1),
	}
	if e.store == nil {
		e.store = NewMemoryStore()
	}
	if len(e.shell) == 0 {
		if runtime.GOOS == "windows" {
			e.shell = []string{"cmd", "/C"}
		} else {
			e.shell = []string{"/bin/sh", "-c"}
		}
	}
	if e.logger == nil {
		// Flag-less: the engine prepends its own RFC3339Nano timestamp in logf.
		e.logger = log.New(os.Stderr, "", 0)
	}
	if e.stdout == nil {
		e.stdout = os.Stdout
	}
	if e.stderr == nil {
		e.stderr = os.Stderr
	}
	if e.now == nil {
		e.now = time.Now
	}
	return e
}

// Register adds a Go step handler. See Registry.Register.
func (e *Engine) Register(name string, fn HandlerFunc) { e.registry.Register(name, fn) }

// HandlerNames returns the registered handler names.
func (e *Engine) HandlerNames() []string { return e.registry.Names() }

// AddJob validates and registers a job. It returns an error if the job is
// malformed, duplicates an existing job, has an invalid schedule, or would
// introduce a dependency cycle.
func (e *Engine) AddJob(j *Job) error {
	if j == nil {
		return fmt.Errorf("engine: nil job")
	}
	if err := validateJob(j); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, dup := e.jobs[j.Name]; dup {
		return fmt.Errorf("engine: duplicate job %q", j.Name)
	}
	if j.Schedule != "" {
		sched, err := cron.Parse(j.Schedule)
		if err != nil {
			return err
		}
		j.compiled = sched
	}

	// Tentatively add, then validate the whole graph; roll back on cycle.
	e.jobs[j.Name] = j
	if err := validateGraph(e.jobs); err != nil {
		delete(e.jobs, j.Name)
		return err
	}
	e.order = append(e.order, j.Name)
	if e.started && j.compiled != nil {
		e.nextFire[j.Name] = j.compiled.Next(e.now())
		// Nudge a running Run loop to recompute its sleep deadline.
		select {
		case e.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func validateJob(j *Job) error {
	if j.Name == "" {
		return fmt.Errorf("engine: job has empty name")
	}
	if len(j.Steps) == 0 {
		return fmt.Errorf("engine: job %q has no steps", j.Name)
	}
	seen := make(map[string]bool, len(j.Steps))
	for i, s := range j.Steps {
		if s.Name == "" {
			return fmt.Errorf("engine: job %q step %d has empty name", j.Name, i)
		}
		if seen[s.Name] {
			return fmt.Errorf("engine: job %q has duplicate step name %q", j.Name, s.Name)
		}
		seen[s.Name] = true
		hasCmd := s.Command != ""
		hasHandler := s.Handler != ""
		if hasCmd == hasHandler {
			return fmt.Errorf("engine: job %q step %q must set exactly one of command/handler", j.Name, s.Name)
		}
	}

	// Validate step-level dependencies: each must reference another step in
	// this job, with no self-reference and no cycles.
	for _, s := range j.Steps {
		for _, dep := range s.DependsOn {
			if dep == s.Name {
				return fmt.Errorf("engine: job %q step %q depends on itself", j.Name, s.Name)
			}
			if !seen[dep] {
				return fmt.Errorf("engine: job %q step %q depends on unknown step %q", j.Name, s.Name, dep)
			}
		}
	}
	if err := detectCycle(effectiveStepDeps(j), fmt.Sprintf("job %q step", j.Name)); err != nil {
		return fmt.Errorf("engine: %w", err)
	}
	return nil
}

// Job returns a copy of the named job's definition.
func (e *Engine) Job(name string) (*Job, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[name]
	if !ok {
		return nil, false
	}
	cp := *j
	cp.Steps = append([]Step(nil), j.Steps...)
	cp.DependsOn = append([]string(nil), j.DependsOn...)
	return &cp, true
}

// JobStatus is a snapshot of a job for reporting.
type JobStatus struct {
	Name      string
	Schedule  string
	DependsOn []string
	NextFire  time.Time // zero if unscheduled
	Running   bool
	Latest    *Run // nil if never run
}

// Latest returns a copy of the most recent run for a job, if any.
func (e *Engine) Latest(name string) (*Run, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.latest[name]
	return cloneRun(r), r != nil
}

// Snapshot returns the current status of all jobs in insertion order.
func (e *Engine) Snapshot() []JobStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]JobStatus, 0, len(e.order))
	for _, name := range e.order {
		j := e.jobs[name]
		st := JobStatus{
			Name:      j.Name,
			Schedule:  j.Schedule,
			DependsOn: append([]string(nil), j.DependsOn...),
			NextFire:  e.nextFire[name],
			Running:   e.running[name],
			Latest:    cloneRun(e.latest[name]),
		}
		out = append(out, st)
	}
	return out
}

// LoadState reads persisted runs into memory so Snapshot/Latest reflect prior
// runs. Run calls it automatically; CLI inspection commands call it directly.
func (e *Engine) LoadState() error { return e.loadState() }

// loadState reads persisted runs into memory. Safe to call before Run.
func (e *Engine) loadState() error {
	runs, err := e.store.Load()
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, r := range runs {
		if _, known := e.jobs[name]; known {
			e.latest[name] = r
		}
	}
	return nil
}

// Run starts the scheduling loop and blocks until ctx is cancelled. On
// cancellation it waits for in-flight runs to finish, then returns ctx.Err().
func (e *Engine) Run(ctx context.Context) error {
	if err := e.loadState(); err != nil {
		return err
	}

	e.mu.Lock()
	e.started = true
	now := e.now()
	for _, name := range e.order {
		if j := e.jobs[name]; j.compiled != nil {
			e.nextFire[name] = j.compiled.Next(now)
		}
	}
	e.mu.Unlock()

	e.logf("scheduler started with %d job(s)", len(e.order))

	for {
		// Sleep precisely until the soonest scheduled fire time so jobs run on
		// their wall-clock boundary (e.g. the top of the minute), rather than
		// polling. If no jobs are scheduled, wait only for shutdown or a new
		// job being added.
		var timerC <-chan time.Time
		var timer *time.Timer
		if next, ok := e.soonestFire(); ok {
			d := next.Sub(e.now())
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			e.logf("scheduler stopping; waiting for in-flight runs")
			e.wg.Wait()
			return ctx.Err()
		case <-e.wake:
			// A job was added (or its schedule changed); recompute the deadline.
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
			e.fireDue(ctx)
		}
	}
}

// soonestFire returns the earliest pending fire time across scheduled jobs.
func (e *Engine) soonestFire() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var soonest time.Time
	found := false
	for _, name := range e.order {
		if e.jobs[name].compiled == nil {
			continue
		}
		nf, ok := e.nextFire[name]
		if !ok {
			continue
		}
		if !found || nf.Before(soonest) {
			soonest, found = nf, true
		}
	}
	return soonest, found
}

// fireDue launches every scheduled job whose fire time has arrived and advances
// its next fire time.
func (e *Engine) fireDue(ctx context.Context) {
	now := e.now()
	var due []*Job
	e.mu.Lock()
	for _, name := range e.order {
		j := e.jobs[name]
		if j.compiled == nil {
			continue
		}
		if nf, ok := e.nextFire[name]; ok && !now.Before(nf) {
			due = append(due, j)
			e.nextFire[name] = j.compiled.Next(now)
		}
	}
	e.mu.Unlock()

	for _, j := range due {
		e.launchAsync(ctx, j, TriggerCron, "", false)
	}
}

// launchAsync starts a run in the background (scheduler path). When
// bypassGating is false and the job has dependencies, the run is skipped
// unless all dependencies most-recently succeeded. On success it cascades to
// dependent jobs.
func (e *Engine) launchAsync(ctx context.Context, job *Job, trigger Trigger, fromStep string, bypassGating bool) {
	if !bypassGating && len(job.DependsOn) > 0 && !e.dependenciesSatisfied(job) {
		e.recordSkip(job, trigger, "dependencies not satisfied")
		return
	}
	if !e.tryStart(job.Name) {
		e.logf("job %q already running; skipping %s trigger", job.Name, trigger)
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.finishStart(job.Name)
		run := e.executeRun(ctx, job, trigger, fromStep)
		if run.Status == StatusSucceeded {
			e.cascade(ctx, job.Name)
		}
	}()
}

// cascade triggers schedule-less dependents of target whose dependencies are
// now satisfied.
func (e *Engine) cascade(ctx context.Context, target string) {
	e.mu.Lock()
	deps := dependents(e.jobs, target)
	var ready []*Job
	for _, name := range deps {
		d := e.jobs[name]
		if d.compiled != nil { // scheduled jobs run on their own cadence
			continue
		}
		if e.running[name] {
			continue
		}
		ready = append(ready, d)
	}
	e.mu.Unlock()

	for _, d := range ready {
		if e.dependenciesSatisfied(d) {
			e.logf("dependency cascade: %q satisfied, triggering %q", target, d.Name)
			e.launchAsync(ctx, d, TriggerDependency, "", false)
		}
	}
}

// Trigger runs a job synchronously now, bypassing dependency gating (an
// explicit manual run). It returns the completed run. It does not cascade to
// dependents — that is a scheduler behavior. Returns an error if the job is
// unknown or already running.
func (e *Engine) Trigger(ctx context.Context, name string) (*Run, error) {
	return e.triggerFrom(ctx, name, "", TriggerManual)
}

// Restart re-runs a job synchronously. fromStep selects where to resume:
//   - "" or "top": re-run every step
//   - a step name: re-run that step
//   - a 1-based index as a string: re-run the step at that position
//
// The chosen step and every step that (transitively) depends on it are
// re-executed. All other steps are presumed to have completed in the prior run
// and are recorded as skipped (and treated as satisfied dependencies).
func (e *Engine) Restart(ctx context.Context, name, fromStep string) (*Run, error) {
	stepName, err := e.resolveStep(name, fromStep)
	if err != nil {
		return nil, err
	}
	return e.triggerFrom(ctx, name, stepName, TriggerRestart)
}

func (e *Engine) triggerFrom(ctx context.Context, name, fromStep string, trigger Trigger) (*Run, error) {
	e.mu.Lock()
	job, ok := e.jobs[name]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("engine: unknown job %q", name)
	}
	if !e.tryStart(name) {
		return nil, fmt.Errorf("engine: job %q is already running", name)
	}
	defer e.finishStart(name)
	return e.executeRun(ctx, job, trigger, fromStep), nil
}

// resolveStep maps a fromStep selector to a step name. "" or "top" returns ""
// (a full run). A 1-based index is converted to the corresponding step name.
func (e *Engine) resolveStep(jobName, fromStep string) (string, error) {
	job, ok := e.Job(jobName)
	if !ok {
		return "", fmt.Errorf("engine: unknown job %q", jobName)
	}
	if fromStep == "" || fromStep == "top" {
		return "", nil
	}
	for _, s := range job.Steps {
		if s.Name == fromStep {
			return s.Name, nil
		}
	}
	if n, err := strconv.Atoi(fromStep); err == nil {
		if n < 1 || n > len(job.Steps) {
			return "", fmt.Errorf("engine: step index %d out of range 1..%d for job %q", n, len(job.Steps), jobName)
		}
		return job.Steps[n-1].Name, nil
	}
	return "", fmt.Errorf("engine: job %q has no step %q", jobName, fromStep)
}

// dependenciesSatisfied reports whether every dependency's latest run
// succeeded.
func (e *Engine) dependenciesSatisfied(job *Job) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, dep := range job.DependsOn {
		r := e.latest[dep]
		if r == nil || r.Status != StatusSucceeded {
			return false
		}
	}
	return true
}

func (e *Engine) tryStart(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running[name] {
		return false
	}
	e.running[name] = true
	return true
}

func (e *Engine) finishStart(name string) {
	e.mu.Lock()
	delete(e.running, name)
	e.mu.Unlock()
}

// stepState tracks a step's progress within a single run. "passed" means the
// step satisfies its dependents (succeeded, tolerated-failure, or presumed
// done on restart); "failed"/"blocked" do not.
type stepState int

const (
	stPending stepState = iota
	stRunning
	stPassed
	stFailed
	stBlocked
)

// stepResult carries a finished step back from its worker goroutine.
type stepResult struct {
	idx int
	sr  StepRun
}

// executeRun runs a job's steps as a DAG and returns the final run. When
// fromStep is non-empty, only that step and its transitive dependents execute;
// the rest are presumed done. Independent ready steps run concurrently.
func (e *Engine) executeRun(ctx context.Context, job *Job, trigger Trigger, fromStep string) *Run {
	start := e.now()
	run := &Run{
		JobName:   job.Name,
		ID:        newRunID(job.Name, start),
		Status:    StatusRunning,
		Trigger:   trigger,
		StartedAt: start,
		FromStep:  fromStep,
		Steps:     make([]StepRun, len(job.Steps)),
	}

	deps := effectiveStepDeps(job)
	var runSet map[string]bool // nil => run all steps
	if fromStep != "" {
		runSet = transitiveStepClosure(deps, fromStep)
	}
	inRun := func(name string) bool { return runSet == nil || runSet[name] }

	idxOf := make(map[string]int, len(job.Steps))
	state := make(map[string]stepState, len(job.Steps))
	for i, s := range job.Steps {
		idxOf[s.Name] = i
		if inRun(s.Name) {
			run.Steps[i] = StepRun{Name: s.Name, Status: StatusPending}
			state[s.Name] = stPending
		} else {
			// Presumed completed in the prior run; satisfies dependents.
			run.Steps[i] = StepRun{Name: s.Name, Status: StatusSkipped}
			state[s.Name] = stPassed
		}
	}

	toRun := 0
	for _, st := range state {
		if st == stPending {
			toRun++
		}
	}
	if fromStep == "" {
		e.logf("job %q starting (%s, %d step(s))", job.Name, trigger, toRun)
	} else {
		e.logf("job %q starting (%s, from %q: %d step(s))", job.Name, trigger, fromStep, toRun)
	}
	e.persist(run)

	depsPassed := func(name string) bool {
		for _, d := range deps[name] {
			if state[d] != stPassed {
				return false
			}
		}
		return true
	}
	depBlocked := func(name string) bool {
		for _, d := range deps[name] {
			if s := state[d]; s == stFailed || s == stBlocked {
				return true
			}
		}
		return false
	}

	jobFailed := false
	results := make(chan stepResult)
	running := 0

	for {
		// Launch every pending step that is ready (deps passed), and mark as
		// skipped any pending step whose deps can no longer be satisfied.
		progressed := false
		for _, s := range job.Steps {
			i := idxOf[s.Name]
			if state[s.Name] != stPending {
				continue
			}
			switch {
			case depBlocked(s.Name):
				run.Steps[i].Status = StatusSkipped
				run.Steps[i].Error = "skipped: a dependency did not succeed"
				state[s.Name] = stBlocked
				jobFailed = true
				progressed = true
			case ctx.Err() != nil:
				run.Steps[i].Status = StatusSkipped
				run.Steps[i].Error = "scheduler shutting down"
				state[s.Name] = stBlocked
				jobFailed = true
				progressed = true
			case depsPassed(s.Name):
				run.Steps[i].Status = StatusRunning
				run.Steps[i].StartedAt = e.now()
				state[s.Name] = stRunning
				running++
				progressed = true
				go func(idx int, step Step, base StepRun) {
					sr := base
					if err := e.runStep(ctx, &sr, step); err != nil {
						sr.Status = StatusFailed
						sr.Error = err.Error()
					} else {
						sr.Status = StatusSucceeded
					}
					results <- stepResult{idx: idx, sr: sr}
				}(i, s, run.Steps[i])
			}
		}
		if progressed {
			e.persist(run)
		}

		if running == 0 {
			break // no work in flight and nothing newly launched -> done
		}

		res := <-results
		running--
		step := job.Steps[res.idx]
		run.Steps[res.idx] = res.sr
		switch {
		case res.sr.Status == StatusSucceeded:
			state[step.Name] = stPassed
		case step.ContinueOnError:
			// Failure tolerated: record it, but dependents may proceed and the
			// job is not failed by it.
			state[step.Name] = stPassed
			e.logf("job %q step %q failed but continueOnError set; proceeding", job.Name, step.Name)
		default:
			state[step.Name] = stFailed
			jobFailed = true
			e.logf("job %q step %q failed: %s", job.Name, step.Name, res.sr.Error)
		}
		e.persist(run)
	}

	if jobFailed {
		run.Status = StatusFailed
	} else {
		run.Status = StatusSucceeded
	}
	run.FinishedAt = e.now()
	e.persist(run)
	e.logf("job %q finished: %s (%s)", job.Name, run.Status, run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond))
	return run
}

// runStep executes a single step with retries, recording attempts in sr.
func (e *Engine) runStep(ctx context.Context, sr *StepRun, step Step) error {
	sr.Status = StatusRunning
	sr.StartedAt = e.now()
	defer func() { sr.FinishedAt = e.now() }()

	attempts := step.Retries + 1
	var err error
	for a := 1; a <= attempts; a++ {
		sr.Attempts = a
		err = e.execStep(ctx, step)
		if err == nil {
			return nil
		}
		if a < attempts {
			e.logf("step %q attempt %d/%d failed: %v", step.Name, a, attempts, err)
			if step.RetryDelay > 0 {
				select {
				case <-time.After(step.RetryDelay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return err
}

// execStep dispatches a single attempt to a command or handler.
func (e *Engine) execStep(ctx context.Context, step Step) error {
	if step.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, step.Timeout)
		defer cancel()
	}
	if step.Handler != "" {
		return e.registry.call(ctx, step)
	}
	return e.runCommand(ctx, step.Command)
}

// runCommand runs a shell command line via the configured shell.
func (e *Engine) runCommand(ctx context.Context, command string) error {
	args := append(append([]string(nil), e.shell[1:]...), command)
	cmd := exec.CommandContext(ctx, e.shell[0], args...)
	cmd.Stdout = e.stdout
	cmd.Stderr = e.stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

// recordSkip persists a skipped run for a job whose dependencies were not met.
func (e *Engine) recordSkip(job *Job, trigger Trigger, note string) {
	now := e.now()
	run := &Run{
		JobName:    job.Name,
		ID:         newRunID(job.Name, now),
		Status:     StatusSkipped,
		Trigger:    trigger,
		StartedAt:  now,
		FinishedAt: now,
		Note:       note,
		Steps:      make([]StepRun, 0),
	}
	e.logf("job %q skipped: %s", job.Name, note)
	e.persist(run)
}

// persist updates the in-memory latest run and writes it to the store.
func (e *Engine) persist(run *Run) {
	e.mu.Lock()
	e.latest[run.JobName] = cloneRun(run)
	e.mu.Unlock()
	if err := e.store.Save(run); err != nil {
		e.logf("warning: failed to persist run for %q: %v", run.JobName, err)
	}
}

func (e *Engine) logf(format string, args ...any) {
	if e.logger != nil {
		// Prepend an ISO-8601 / RFC3339Nano timestamp from the engine clock.
		e.logger.Print(e.now().Format(time.RFC3339Nano) + " " + fmt.Sprintf(format, args...))
	}
}

func newRunID(job string, t time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%x", job, t.UTC().Format("20060102T150405"), b)
}

// SortedJobNames returns job names sorted alphabetically (helper for callers).
func (e *Engine) SortedJobNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]string(nil), e.order...)
	sort.Strings(out)
	return out
}
