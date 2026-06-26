// Package dsl provides a compact, indentation-based domain-specific language
// for jobflow job definitions, with lossless conversion to and from the JSON
// config format consumed by package config.
//
// The DSL trades JSON's punctuation for whitespace structure. For example:
//
//	job build
//	  every 1m
//	  step compile
//	    run make linux
//	  parallel
//	    step test-unit
//	      handler noop
//	    step test-int
//	      handler noop
//
// Use ParseDSL to turn DSL text into a Document, Document.JSON to emit the
// config JSON, FromJSON to read config JSON back into a Document, and
// Document.DSL to render a Document as canonical DSL text. JSON<->DSL round
// trips are stable.
package dsl

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Document is the parsed representation of a config, shared by the DSL and JSON
// sides.
type Document struct {
	Shell   []string // optional shell command vector
	NoWarn  []string // optional warning codes to silence (or "all")
	Runners []Runner // optional named interpreters/remote targets
	Jobs    []Job
}

// Runner is a named interpreter or remote SSH target.
type Runner struct {
	Name  string
	SSH   []string
	Shell []string
}

// Job is a named, optionally scheduled unit composed of ordered stages.
type Job struct {
	Name     string
	Schedule string   // cron spec, e.g. "@every 1m" or "0 2 * * *"; empty if none
	Needs    []string // job-level dependencies
	Runner   string   // default runner name for command steps; empty = local
	Stages   []Stage
}

// Stage is one position in a job's ordered step list: either a single step or a
// parallel group. When Parallel is false, Steps has exactly one element.
type Stage struct {
	Parallel bool
	Steps    []Step
}

// Step is a single unit of work. Exactly one of Command or Handler is set.
type Step struct {
	Name            string
	Needs           []string // step-level dependencies (advanced DAGs)
	Command         string
	Handler         string
	Runner          string // runner name override for this command step
	Args            []string
	Retries         int
	RetryDelay      string // Go duration string, e.g. "30s"
	Timeout         string
	ContinueOnError bool
	Stdin           string
	Stdout          string
	StdoutAppend    bool
	Stderr          string
	StderrAppend    bool
}

// --- JSON shapes mirroring package config's on-disk format ---

type jStep struct {
	Name            string   `json:"name"`
	DependsOn       []string `json:"dependsOn,omitempty"`
	Command         string   `json:"command,omitempty"`
	Handler         string   `json:"handler,omitempty"`
	Args            []string `json:"args,omitempty"`
	Retries         int      `json:"retries,omitempty"`
	RetryDelay      string   `json:"retryDelay,omitempty"`
	Timeout         string   `json:"timeout,omitempty"`
	ContinueOnError bool     `json:"continueOnError,omitempty"`
	Runner          string   `json:"runner,omitempty"`
	Stdin           string   `json:"stdin,omitempty"`
	Stdout          string   `json:"stdout,omitempty"`
	StdoutAppend    bool     `json:"stdoutAppend,omitempty"`
	Stderr          string   `json:"stderr,omitempty"`
	StderrAppend    bool     `json:"stderrAppend,omitempty"`
}

type jParallel struct {
	Parallel []jStep `json:"parallel"`
}

type jRunner struct {
	SSH   []string `json:"ssh,omitempty"`
	Shell []string `json:"shell,omitempty"`
}

type jJob struct {
	Name      string   `json:"name"`
	Schedule  string   `json:"schedule,omitempty"`
	DependsOn []string `json:"dependsOn,omitempty"`
	Runner    string   `json:"runner,omitempty"`
	Steps     []any    `json:"steps"`
}

type jFile struct {
	Shell   []string           `json:"shell,omitempty"`
	NoWarn  []string           `json:"noWarn,omitempty"`
	Runners map[string]jRunner `json:"runners,omitempty"`
	Jobs    []jJob             `json:"jobs"`
}

func (s Step) toJSON() jStep {
	return jStep{
		Name:            s.Name,
		DependsOn:       s.Needs,
		Command:         s.Command,
		Handler:         s.Handler,
		Args:            s.Args,
		Retries:         s.Retries,
		RetryDelay:      s.RetryDelay,
		Timeout:         s.Timeout,
		ContinueOnError: s.ContinueOnError,
		Runner:          s.Runner,
		Stdin:           s.Stdin,
		Stdout:          s.Stdout,
		StdoutAppend:    s.StdoutAppend,
		Stderr:          s.Stderr,
		StderrAppend:    s.StderrAppend,
	}
}

func stepFromJSON(j jStep) Step {
	return Step{
		Name:            j.Name,
		Needs:           j.DependsOn,
		Command:         j.Command,
		Handler:         j.Handler,
		Args:            j.Args,
		Retries:         j.Retries,
		RetryDelay:      j.RetryDelay,
		Timeout:         j.Timeout,
		ContinueOnError: j.ContinueOnError,
		Runner:          j.Runner,
		Stdin:           j.Stdin,
		Stdout:          j.Stdout,
		StdoutAppend:    j.StdoutAppend,
		Stderr:          j.Stderr,
		StderrAppend:    j.StderrAppend,
	}
}

// JSON renders the document as indented config JSON (the format read by
// package config and the jobflow CLI).
func (d *Document) JSON() ([]byte, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	f := jFile{Shell: d.Shell, NoWarn: d.NoWarn}
	if len(d.Runners) > 0 {
		f.Runners = make(map[string]jRunner, len(d.Runners))
		for _, r := range d.Runners {
			f.Runners[r.Name] = jRunner{SSH: r.SSH, Shell: r.Shell}
		}
	}
	for _, job := range d.Jobs {
		jj := jJob{Name: job.Name, Schedule: job.Schedule, DependsOn: job.Needs, Runner: job.Runner}
		for _, st := range job.Stages {
			if st.Parallel {
				grp := jParallel{}
				for _, s := range st.Steps {
					grp.Parallel = append(grp.Parallel, s.toJSON())
				}
				jj.Steps = append(jj.Steps, grp)
			} else {
				jj.Steps = append(jj.Steps, st.Steps[0].toJSON())
			}
		}
		f.Jobs = append(f.Jobs, jj)
	}
	out, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// FromJSON parses config JSON into a Document.
func FromJSON(data []byte) (*Document, error) {
	var in struct {
		Shell   []string           `json:"shell"`
		NoWarn  []string           `json:"noWarn"`
		Runners map[string]jRunner `json:"runners"`
		Jobs    []struct {
			Name      string            `json:"name"`
			Schedule  string            `json:"schedule"`
			DependsOn []string          `json:"dependsOn"`
			Runner    string            `json:"runner"`
			Steps     []json.RawMessage `json:"steps"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("dsl: parse JSON: %w", err)
	}

	doc := &Document{Shell: in.Shell, NoWarn: in.NoWarn}
	for _, name := range sortedKeys(in.Runners) {
		r := in.Runners[name]
		doc.Runners = append(doc.Runners, Runner{Name: name, SSH: r.SSH, Shell: r.Shell})
	}
	for _, j := range in.Jobs {
		job := Job{Name: j.Name, Schedule: j.Schedule, Needs: j.DependsOn, Runner: j.Runner}
		for i, raw := range j.Steps {
			var probe struct {
				Parallel []jStep `json:"parallel"`
			}
			if err := json.Unmarshal(raw, &probe); err == nil && probe.Parallel != nil {
				stage := Stage{Parallel: true}
				for _, js := range probe.Parallel {
					stage.Steps = append(stage.Steps, stepFromJSON(js))
				}
				job.Stages = append(job.Stages, stage)
				continue
			}
			var js jStep
			if err := json.Unmarshal(raw, &js); err != nil {
				return nil, fmt.Errorf("dsl: job %q stage %d: %w", j.Name, i+1, err)
			}
			job.Stages = append(job.Stages, Stage{Steps: []Step{stepFromJSON(js)}})
		}
		doc.Jobs = append(doc.Jobs, job)
	}
	return doc, nil
}

// sortedKeys returns the keys of a runner map in sorted order, for
// deterministic Document construction from JSON.
func sortedKeys(m map[string]jRunner) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// validate checks structural invariants shared by both output paths.
func (d *Document) validate() error {
	for _, job := range d.Jobs {
		if job.Name == "" {
			return fmt.Errorf("dsl: a job is missing a name")
		}
		if len(job.Stages) == 0 {
			return fmt.Errorf("dsl: job %q has no steps", job.Name)
		}
		for _, st := range job.Stages {
			if st.Parallel && len(st.Steps) == 0 {
				return fmt.Errorf("dsl: job %q has an empty parallel group", job.Name)
			}
			for _, s := range st.Steps {
				if s.Name == "" {
					return fmt.Errorf("dsl: job %q has a step with no name", job.Name)
				}
				if (s.Command == "") == (s.Handler == "") {
					return fmt.Errorf("dsl: job %q step %q must set exactly one of run/handler", job.Name, s.Name)
				}
				if s.RetryDelay != "" {
					if _, err := time.ParseDuration(s.RetryDelay); err != nil {
						return fmt.Errorf("dsl: job %q step %q retry-delay %q: %w", job.Name, s.Name, s.RetryDelay, err)
					}
				}
				if s.Timeout != "" {
					if _, err := time.ParseDuration(s.Timeout); err != nil {
						return fmt.Errorf("dsl: job %q step %q timeout %q: %w", job.Name, s.Name, s.Timeout, err)
					}
				}
			}
		}
	}
	return nil
}
