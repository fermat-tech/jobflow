package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// load parses a JSON document into engine jobs via the normal path.
func load(t *testing.T, doc string) []engineStep {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	jobs, err := f.EngineJobs()
	if err != nil {
		t.Fatalf("EngineJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("want 1 job, got %d", len(jobs))
	}
	var out []engineStep
	for _, s := range jobs[0].Steps {
		out = append(out, engineStep{name: s.Name, deps: s.DependsOn})
	}
	return out
}

type engineStep struct {
	name string
	deps []string
}

func depsOf(steps []engineStep, name string) []string {
	for _, s := range steps {
		if s.name == name {
			return s.deps
		}
	}
	return nil
}

func eqSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		if !m[v] {
			return false
		}
	}
	return true
}

func TestParallelGroupLowering(t *testing.T) {
	doc := `{"jobs":[{"name":"p","steps":[
		{"name":"checkout","command":"x"},
		{"parallel":[
			{"name":"bl","command":"x"},
			{"name":"bw","command":"x"}
		]},
		{"name":"release","command":"x"}
	]}]}`
	steps := load(t, doc)

	if d := depsOf(steps, "checkout"); len(d) != 0 {
		t.Errorf("checkout deps = %v, want none", d)
	}
	if d := depsOf(steps, "bl"); !eqSet(d, []string{"checkout"}) {
		t.Errorf("bl deps = %v, want [checkout]", d)
	}
	if d := depsOf(steps, "bw"); !eqSet(d, []string{"checkout"}) {
		t.Errorf("bw deps = %v, want [checkout]", d)
	}
	if d := depsOf(steps, "release"); !eqSet(d, []string{"bl", "bw"}) {
		t.Errorf("release deps = %v, want [bl bw]", d)
	}
}

func TestSequentialNoGroupsHasNoDeps(t *testing.T) {
	doc := `{"jobs":[{"name":"s","steps":[
		{"name":"a","command":"x"},
		{"name":"b","command":"x"}
	]}]}`
	steps := load(t, doc)
	for _, s := range steps {
		if len(s.deps) != 0 {
			t.Errorf("step %q has deps %v; sequential jobs should stay dep-free", s.name, s.deps)
		}
	}
}

func TestExplicitDependsOnUnionsWithStage(t *testing.T) {
	// A group member with its own dependsOn keeps it, unioned with the stage.
	doc := `{"jobs":[{"name":"p","steps":[
		{"name":"a","command":"x"},
		{"name":"b","command":"x"},
		{"parallel":[
			{"name":"c","command":"x","dependsOn":["a"]}
		]}
	]}]}`
	steps := load(t, doc)
	// Stage before the group is [b]; c also explicitly depends on a.
	if d := depsOf(steps, "c"); !eqSet(d, []string{"b", "a"}) {
		t.Errorf("c deps = %v, want [b a]", d)
	}
}

func TestEmptyParallelGroupRejected(t *testing.T) {
	doc := `{"jobs":[{"name":"p","steps":[{"parallel":[]}]}]}`
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	os.WriteFile(path, []byte(doc), 0o644)
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.EngineJobs(); err == nil {
		t.Fatal("expected error for empty parallel group")
	}
}

// Guard: a single-step entry must not be misread as a group.
func TestStepIsNotGroup(t *testing.T) {
	var g group
	if err := json.Unmarshal([]byte(`{"name":"a","command":"x"}`), &g); err != nil {
		t.Fatal(err)
	}
	if g.Parallel != nil {
		t.Fatal("plain step decoded as a parallel group")
	}
}
