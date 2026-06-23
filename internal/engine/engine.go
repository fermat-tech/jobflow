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

	"github.com/fermat-tech/jobflow/internal/cron"
)

// Options configures a new Engine. The zero value is valid; sensible defaults
// are filled in by New.
type Options struct {
	// Store persists run state. Defaults to an in-memory store.
	Store Store
	// Shell is the command prefix used to run Command steps. Defaults to
	// {"cmd", "/C"} on Windows and {"/bin/sh", "-c"} elsewhere.
	Shell []string
	// Logger receives scheduler activity. Defaults to log.Default().
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

	wg sync.WaitGroup // tracks in-flight async runs
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
		e.logger = log.Default()
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

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.logf("scheduler stopping; waiting for in-flight runs")
			e.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

// tick fires any jobs whose scheduled time has arrived.
func (e *Engine) tick(ctx context.Context) {
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
		e.launchAsync(ctx, j, TriggerCron, 0, false)
	}
}

// launchAsync starts a run in the background (scheduler path). When
// bypassGating is false and the job has dependencies, the run is skipped
// unless all dependencies most-recently succeeded. On success it cascades to
// dependent jobs.
func (e *Engine) launchAsync(ctx context.Context, job *Job, trigger Trigger, fromStep int, bypassGating bool) {
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
			e.launchAsync(ctx, d, TriggerDependency, 0, false)
		}
	}
}

// Trigger runs a job synchronously now, bypassing dependency gating (an
// explicit manual run). It returns the completed run. It does not cascade to
// dependents — that is a scheduler behavior. Returns an error if the job is
// unknown or already running.
func (e *Engine) Trigger(ctx context.Context, name string) (*Run, error) {
	return e.triggerFrom(ctx, name, 0, TriggerManual)
}

// Restart re-runs a job synchronously. fromStep selects where to resume:
//   - "" or "top": from the first step
//   - a step name: from that step
//   - a 1-based index as a string: from that position
//
// Steps before the resume point are recorded as skipped.
func (e *Engine) Restart(ctx context.Context, name, fromStep string) (*Run, error) {
	idx, err := e.resolveStep(name, fromStep)
	if err != nil {
		return nil, err
	}
	return e.triggerFrom(ctx, name, idx, TriggerRestart)
}

func (e *Engine) triggerFrom(ctx context.Context, name string, fromStep int, trigger Trigger) (*Run, error) {
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

// resolveStep maps a fromStep selector to a 0-based step index.
func (e *Engine) resolveStep(jobName, fromStep string) (int, error) {
	job, ok := e.Job(jobName)
	if !ok {
		return 0, fmt.Errorf("engine: unknown job %q", jobName)
	}
	if fromStep == "" || fromStep == "top" {
		return 0, nil
	}
	for i, s := range job.Steps {
		if s.Name == fromStep {
			return i, nil
		}
	}
	if n, err := strconv.Atoi(fromStep); err == nil {
		if n < 1 || n > len(job.Steps) {
			return 0, fmt.Errorf("engine: step index %d out of range 1..%d for job %q", n, len(job.Steps), jobName)
		}
		return n - 1, nil
	}
	return 0, fmt.Errorf("engine: job %q has no step %q", jobName, fromStep)
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

// executeRun runs job's steps starting at fromStep and returns the final run.
func (e *Engine) executeRun(ctx context.Context, job *Job, trigger Trigger, fromStep int) *Run {
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
	for i, s := range job.Steps {
		st := StepRun{Name: s.Name, Status: StatusPending}
		if i < fromStep {
			st.Status = StatusSkipped
		}
		run.Steps[i] = st
	}
	e.logf("job %q starting (%s, %d step(s) from #%d)", job.Name, trigger, len(job.Steps)-fromStep, fromStep+1)
	e.persist(run)

	jobFailed := false
	for i := fromStep; i < len(job.Steps); i++ {
		if ctx.Err() != nil {
			run.Steps[i].Status = StatusSkipped
			run.Steps[i].Error = "scheduler shutting down"
			jobFailed = true
			break
		}
		step := job.Steps[i]
		err := e.runStep(ctx, &run.Steps[i], step)
		if err != nil {
			run.Steps[i].Status = StatusFailed
			run.Steps[i].Error = err.Error()
			e.logf("job %q step %q failed: %v", job.Name, step.Name, err)
			if !step.ContinueOnError {
				jobFailed = true
				e.persist(run)
				break
			}
			e.logf("job %q step %q failed but continueOnError set; proceeding", job.Name, step.Name)
		} else {
			run.Steps[i].Status = StatusSucceeded
		}
		e.persist(run)
	}

	// Any step still pending after an early exit is recorded as skipped.
	for i := range run.Steps {
		if run.Steps[i].Status == StatusPending {
			run.Steps[i].Status = StatusSkipped
		}
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
		e.logger.Printf(format, args...)
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
