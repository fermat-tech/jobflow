// Package engine implements a cron-like job scheduler with multi-step jobs,
// restart-from-step, and inter-job dependencies. It is usable both as an
// embedded library and behind the jobflow CLI.
package engine

import (
	"context"
	"time"

	"github.com/fermat-tech/jobflow/cron"
)

// Status is the lifecycle state of a run or a single step.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusSkipped   Status = "skipped"
)

// Trigger records why a run started.
type Trigger string

const (
	TriggerCron       Trigger = "cron"
	TriggerManual     Trigger = "manual"
	TriggerDependency Trigger = "dependency"
	TriggerRestart    Trigger = "restart"
)

// Step is one unit of work within a job. Exactly one of Command or Handler
// must be set. Steps within a job run sequentially in declaration order.
type Step struct {
	// Name uniquely identifies the step within its job. Used as the target
	// for restart-from-step.
	Name string `json:"name"`

	// Description is optional free-form documentation of the step's purpose.
	Description string `json:"description,omitempty"`

	// DependsOn lists other steps in the same job that must complete before
	// this step runs. If ANY step in a job sets DependsOn, the job runs as a
	// DAG: steps with no declared deps start immediately (in parallel) and
	// each step waits only for its listed deps. If NO step sets DependsOn, the
	// job runs sequentially in declaration order (the default).
	DependsOn []string `json:"dependsOn,omitempty"`

	// Command is a shell command line, executed via the engine's configured
	// shell (cmd /C on Windows, /bin/sh -c elsewhere).
	Command string `json:"command,omitempty"`

	// Handler names a Go handler registered with the engine via Register.
	Handler string `json:"handler,omitempty"`

	// Runner names the runner (interpreter/remote target) for this command
	// step, overriding the job's runner. Command steps only. Empty inherits.
	Runner string `json:"runner,omitempty"`

	// Args are passed through to a Handler step (ignored for Command steps).
	Args []string `json:"args,omitempty"`

	// Retries is the number of additional attempts after the first failure.
	Retries int `json:"retries,omitempty"`

	// RetryDelay is the wait between attempts.
	RetryDelay time.Duration `json:"retryDelay,omitempty"`

	// ContinueOnError lets the job proceed to the next step even if this one
	// ultimately fails. The step is still recorded as failed.
	ContinueOnError bool `json:"continueOnError,omitempty"`

	// Timeout, if > 0, bounds a single attempt of the step.
	Timeout time.Duration `json:"timeout,omitempty"`

	// Stdin, Stdout, and Stderr redirect a Command step's standard streams to
	// files, so the command string needs no shell redirection operators. They
	// are honored only for Command steps. Empty means inherit: Stdin is the
	// null device; Stdout/Stderr use the engine's writers.
	//
	// Stdin is a path opened for reading. StdoutAppend/StderrAppend select
	// append (true) vs truncate (false) for the corresponding output file. If
	// Stdout and Stderr name the same file, they share one handle (like
	// "> f 2>&1").
	Stdin        string `json:"stdin,omitempty"`
	Stdout       string `json:"stdout,omitempty"`
	StdoutAppend bool   `json:"stdoutAppend,omitempty"`
	Stderr       string `json:"stderr,omitempty"`
	StderrAppend bool   `json:"stderrAppend,omitempty"`
}

// Job is a named, scheduled unit composed of ordered steps. A job may have a
// cron schedule, dependencies on other jobs, or both.
type Job struct {
	// Name uniquely identifies the job in the engine.
	Name string `json:"name"`

	// Description is optional free-form documentation of the job's purpose.
	Description string `json:"description,omitempty"`

	// Schedule is an optional cron expression. When empty, the job only runs
	// when triggered manually or by dependency cascade.
	Schedule string `json:"schedule,omitempty"`

	// DependsOn lists jobs that must have most-recently succeeded before this
	// job will execute its steps. See Engine docs for the exact semantics.
	DependsOn []string `json:"dependsOn,omitempty"`

	// Runner names the default runner (interpreter/remote target) for this
	// job's command steps. A step's own Runner overrides it. Empty uses the
	// engine's local shell.
	Runner string `json:"runner,omitempty"`

	// ExplicitSteps makes the steps' DependsOn the authoritative intra-job DAG:
	// no sequential ordering is synthesized, so steps with no deps run as
	// parallel roots even when they are the only steps. Config sets this when a
	// job uses parallel groups. When false (default), the engine runs steps
	// sequentially unless some step declares DependsOn.
	ExplicitSteps bool `json:"explicitSteps,omitempty"`

	// Steps are executed in order.
	Steps []Step `json:"steps"`

	compiled *cron.Schedule // set by AddJob when Schedule != ""
}

// HandlerFunc is a Go-native step implementation registered by name.
type HandlerFunc func(ctx context.Context, step Step) error

// StepRun captures the outcome of a single step within a run.
type StepRun struct {
	Name       string    `json:"name"`
	Status     Status    `json:"status"`
	Attempts   int       `json:"attempts"`
	StartedAt  time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Run captures one execution of a job, including per-step outcomes.
type Run struct {
	JobName    string    `json:"jobName"`
	ID         string    `json:"id"`
	Status     Status    `json:"status"`
	Trigger    Trigger   `json:"trigger"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	Steps      []StepRun `json:"steps"`
	// FromStep names the step a restart resumed from ("" for a normal full run).
	FromStep string `json:"fromStep,omitempty"`
	// Note carries a human-readable explanation, e.g. why a run was skipped.
	Note string `json:"note,omitempty"`
}

// step returns the StepRun with the given name, or nil.
func (r *Run) step(name string) *StepRun {
	for i := range r.Steps {
		if r.Steps[i].Name == name {
			return &r.Steps[i]
		}
	}
	return nil
}
