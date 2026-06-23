package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store persists the latest run per job so the scheduler can resume knowledge
// of past outcomes (needed for dependency gating and restart-from-step) across
// process restarts.
type Store interface {
	// Load returns the latest run for every known job, keyed by job name.
	Load() (map[string]*Run, error)
	// Save persists the latest run for a single job.
	Save(run *Run) error
}

// MemoryStore is an in-memory Store, useful for tests and ephemeral use.
type MemoryStore struct {
	mu   sync.Mutex
	runs map[string]*Run
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[string]*Run)}
}

// Load implements Store.
func (m *MemoryStore) Load() (map[string]*Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*Run, len(m.runs))
	for k, v := range m.runs {
		out[k] = cloneRun(v)
	}
	return out, nil
}

// Save implements Store.
func (m *MemoryStore) Save(run *Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.JobName] = cloneRun(run)
	return nil
}

// FileStore persists state as a single JSON file. Writes are atomic
// (write-temp-then-rename) and serialized by a mutex.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore returns a Store backed by the given JSON file path. The file is
// created lazily on the first Save; a missing file Loads as empty state.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

type fileState struct {
	Version int              `json:"version"`
	Runs    map[string]*Run  `json:"runs"`
}

// Load implements Store.
func (f *FileStore) Load() (map[string]*Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*Run{}, nil
		}
		return nil, fmt.Errorf("store: read %s: %w", f.path, err)
	}
	var st fileState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("store: parse %s: %w", f.path, err)
	}
	if st.Runs == nil {
		st.Runs = map[string]*Run{}
	}
	return st.Runs, nil
}

// Save implements Store. It rewrites the whole file under lock to keep the
// on-disk document internally consistent.
func (f *FileStore) Save(run *Run) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read-modify-write so concurrent jobs don't clobber each other's entries.
	runs := map[string]*Run{}
	if data, err := os.ReadFile(f.path); err == nil {
		var st fileState
		if err := json.Unmarshal(data, &st); err == nil && st.Runs != nil {
			runs = st.Runs
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("store: read %s: %w", f.path, err)
	}

	runs[run.JobName] = cloneRun(run)

	out, err := json.MarshalIndent(fileState{Version: 1, Runs: runs}, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal: %w", err)
	}

	if dir := filepath.Dir(f.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("store: mkdir %s: %w", dir, err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return fmt.Errorf("store: write temp: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

func cloneRun(r *Run) *Run {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Steps = append([]StepRun(nil), r.Steps...)
	return &cp
}
