// Package config loads job definitions from a JSON file and converts them into
// engine jobs. Durations are written as human strings ("30s", "5m") rather
// than raw nanoseconds.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/fermat-tech/jobflow/internal/engine"
)

// File is the top-level config document.
type File struct {
	// Shell overrides the command interpreter for Command steps, e.g.
	// ["pwsh", "-NoProfile", "-Command"]. Optional.
	Shell []string `json:"shell,omitempty"`
	// Jobs are the job definitions.
	Jobs []Job `json:"jobs"`
}

// Job mirrors engine.Job with string durations on its steps.
type Job struct {
	Name      string   `json:"name"`
	Schedule  string   `json:"schedule,omitempty"`
	DependsOn []string `json:"dependsOn,omitempty"`
	Steps     []Step   `json:"steps"`
}

// Step mirrors engine.Step but takes durations as strings.
type Step struct {
	Name            string   `json:"name"`
	DependsOn       []string `json:"dependsOn,omitempty"`
	Command         string   `json:"command,omitempty"`
	Handler         string   `json:"handler,omitempty"`
	Args            []string `json:"args,omitempty"`
	Retries         int      `json:"retries,omitempty"`
	RetryDelay      string   `json:"retryDelay,omitempty"`
	Timeout         string   `json:"timeout,omitempty"`
	ContinueOnError bool     `json:"continueOnError,omitempty"`
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

// EngineJobs converts the config's jobs into engine jobs, parsing durations.
func (f *File) EngineJobs() ([]*engine.Job, error) {
	out := make([]*engine.Job, 0, len(f.Jobs))
	for _, j := range f.Jobs {
		ej := &engine.Job{
			Name:      j.Name,
			Schedule:  j.Schedule,
			DependsOn: j.DependsOn,
		}
		for _, s := range j.Steps {
			es := engine.Step{
				Name:            s.Name,
				DependsOn:       s.DependsOn,
				Command:         s.Command,
				Handler:         s.Handler,
				Args:            s.Args,
				Retries:         s.Retries,
				ContinueOnError: s.ContinueOnError,
			}
			if s.RetryDelay != "" {
				d, err := time.ParseDuration(s.RetryDelay)
				if err != nil {
					return nil, fmt.Errorf("config: job %q step %q retryDelay: %w", j.Name, s.Name, err)
				}
				es.RetryDelay = d
			}
			if s.Timeout != "" {
				d, err := time.ParseDuration(s.Timeout)
				if err != nil {
					return nil, fmt.Errorf("config: job %q step %q timeout: %w", j.Name, s.Name, err)
				}
				es.Timeout = d
			}
			ej.Steps = append(ej.Steps, es)
		}
		out = append(out, ej)
	}
	return out, nil
}
