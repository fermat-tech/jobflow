// Package config loads job definitions from a JSON file and converts them into
// engine jobs. Durations are written as human strings ("30s", "5m") rather
// than raw nanoseconds.
//
// A job's "steps" is an ordered list of stages. Each stage is either a single
// step object or a parallel group:
//
//	"steps": [
//	  { "name": "checkout", "command": "git pull" },
//	  { "parallel": [
//	      { "name": "build-linux",   "command": "..." },
//	      { "name": "build-windows", "command": "..." }
//	  ]},
//	  { "name": "release", "command": "..." }
//	]
//
// Stages run in order; the steps inside a parallel group run concurrently, and
// the following stage waits for all of them. This is sugar over the engine's
// step-level dependsOn: groups are lowered to dependencies on the prior stage.
// A step may still set "dependsOn" explicitly for arbitrary (non-staged) DAGs.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/fermat-tech/jobflow/engine"
)

// File is the top-level config document.
type File struct {
	// Shell overrides the command interpreter for Command steps, e.g.
	// ["pwsh", "-NoProfile", "-Command"]. Optional.
	Shell []string `json:"shell,omitempty"`
	// NoWarn lists warning codes to silence (or "all"). Optional.
	NoWarn []string `json:"noWarn,omitempty"`
	// Runners are named interpreters/remote targets, keyed by name. Optional.
	Runners map[string]Runner `json:"runners,omitempty"`
	// Jobs are the job definitions.
	Jobs []Job `json:"jobs"`
}

// Runner is a named interpreter or remote SSH target. When SSH is set the
// command runs remotely; Shell is the (remote or local) interpreter.
type Runner struct {
	SSH   []string `json:"ssh,omitempty"`
	Shell []string `json:"shell,omitempty"`
}

// Job mirrors engine.Job. Steps holds raw stage entries decoded per-element.
type Job struct {
	Name      string            `json:"name"`
	Schedule  string            `json:"schedule,omitempty"`
	DependsOn []string          `json:"dependsOn,omitempty"`
	Runner    string            `json:"runner,omitempty"`
	Steps     []json.RawMessage `json:"steps"`
}

// Step mirrors engine.Step but takes durations as strings.
type Step struct {
	Name            string   `json:"name"`
	DependsOn       []string `json:"dependsOn,omitempty"`
	Command         string   `json:"command,omitempty"`
	Handler         string   `json:"handler,omitempty"`
	Runner          string   `json:"runner,omitempty"`
	Args            []string `json:"args,omitempty"`
	Retries         int      `json:"retries,omitempty"`
	RetryDelay      string   `json:"retryDelay,omitempty"`
	Timeout         string   `json:"timeout,omitempty"`
	ContinueOnError bool     `json:"continueOnError,omitempty"`
	Stdin           string   `json:"stdin,omitempty"`
	Stdout          string   `json:"stdout,omitempty"`
	StdoutAppend    bool     `json:"stdoutAppend,omitempty"`
	Stderr          string   `json:"stderr,omitempty"`
	StderrAppend    bool     `json:"stderrAppend,omitempty"`
}

// group is a parallel stage: { "parallel": [ ...steps... ] }.
type group struct {
	Parallel []Step `json:"parallel"`
}

// Load reads and parses a config file.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return &f, nil
}

// EngineJobs converts the config's jobs into engine jobs, lowering parallel
// stage groups onto step-level dependencies and parsing durations.
func (f *File) EngineJobs() ([]*engine.Job, error) {
	out := make([]*engine.Job, 0, len(f.Jobs))
	for _, j := range f.Jobs {
		ej := &engine.Job{
			Name:      j.Name,
			Schedule:  j.Schedule,
			DependsOn: j.DependsOn,
			Runner:    j.Runner,
		}

		// First pass: decode each stage entry and detect whether any groups
		// are present (groups force explicit stage ordering).
		type stage struct {
			steps []Step
		}
		var stages []stage
		hasGroup := false
		for i, raw := range j.Steps {
			var g group
			if err := json.Unmarshal(raw, &g); err == nil && g.Parallel != nil {
				if len(g.Parallel) == 0 {
					return nil, fmt.Errorf("config: job %q stage %d: empty parallel group", j.Name, i+1)
				}
				hasGroup = true
				stages = append(stages, stage{steps: g.Parallel})
				continue
			}
			var s Step
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, fmt.Errorf("config: job %q stage %d: %w", j.Name, i+1, err)
			}
			stages = append(stages, stage{steps: []Step{s}})
		}

		// Second pass: lower stages to engine steps. When any group exists,
		// each stage depends on every step of the prior stage (preserving
		// order); otherwise steps pass through (sequential by engine default).
		var prevStage []string
		for _, st := range stages {
			var names []string
			for _, s := range st.steps {
				es, err := toEngineStep(j.Name, s)
				if err != nil {
					return nil, err
				}
				if hasGroup {
					es.DependsOn = union(prevStage, s.DependsOn)
				}
				ej.Steps = append(ej.Steps, es)
				names = append(names, s.Name)
			}
			prevStage = names
		}

		out = append(out, ej)
	}
	return out, nil
}

// EngineRunners converts the config's named runners into engine runners.
func (f *File) EngineRunners() []engine.Runner {
	out := make([]engine.Runner, 0, len(f.Runners))
	for name, r := range f.Runners {
		out = append(out, engine.Runner{Name: name, SSH: r.SSH, Shell: r.Shell})
	}
	return out
}

// toEngineStep converts a config Step to an engine Step, parsing durations.
func toEngineStep(jobName string, s Step) (engine.Step, error) {
	es := engine.Step{
		Name:            s.Name,
		DependsOn:       s.DependsOn,
		Command:         s.Command,
		Handler:         s.Handler,
		Runner:          s.Runner,
		Args:            s.Args,
		Retries:         s.Retries,
		ContinueOnError: s.ContinueOnError,
		Stdin:           s.Stdin,
		Stdout:          s.Stdout,
		StdoutAppend:    s.StdoutAppend,
		Stderr:          s.Stderr,
		StderrAppend:    s.StderrAppend,
	}
	if s.RetryDelay != "" {
		d, err := time.ParseDuration(s.RetryDelay)
		if err != nil {
			return engine.Step{}, fmt.Errorf("config: job %q step %q retryDelay: %w", jobName, s.Name, err)
		}
		es.RetryDelay = d
	}
	if s.Timeout != "" {
		d, err := time.ParseDuration(s.Timeout)
		if err != nil {
			return engine.Step{}, fmt.Errorf("config: job %q step %q timeout: %w", jobName, s.Name, err)
		}
		es.Timeout = d
	}
	return es, nil
}

// union returns the deduplicated concatenation of a and b, preserving order.
func union(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(a)+len(b))
	var out []string
	for _, v := range append(append([]string(nil), a...), b...) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
