package dsl_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fermat-tech/jobflow/config"
	"github.com/fermat-tech/jobflow/dsl"
	"github.com/fermat-tech/jobflow/engine"
)

const sample = `shell pwsh -NoProfile -Command

job build
  every 1m
  step compile
    run make linux
  parallel
    step test-unit
      handler noop
    step test-int
      handler log running "two words"
      retries 2
      retry-delay 1s

job deploy
  schedule 0 2 * * *
  needs build
  step ship
    run echo shipping
    timeout 30s
    continue-on-error
  step verify
    handler check ok
    needs ship
`

func TestRoundTripStable(t *testing.T) {
	doc1, err := dsl.ParseDSL(sample)
	if err != nil {
		t.Fatalf("ParseDSL: %v", err)
	}

	// The sample is already canonical, so re-dumping it must reproduce it.
	if got := doc1.DSL(); got != sample {
		t.Fatalf("DSL not canonical.\n--- got ---\n%s\n--- want ---\n%s", got, sample)
	}

	json1, err := doc1.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}

	// JSON -> Document -> DSL -> Document -> JSON must be byte-stable.
	doc2, err := dsl.FromJSON(json1)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	dsl2 := doc2.DSL()
	if dsl2 != sample {
		t.Fatalf("JSON->DSL drifted.\n--- got ---\n%s", dsl2)
	}
	doc3, err := dsl.ParseDSL(dsl2)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	json3, err := doc3.JSON()
	if err != nil {
		t.Fatalf("JSON #3: %v", err)
	}
	if string(json1) != string(json3) {
		t.Fatalf("JSON not stable across round trip.\n--- json1 ---\n%s\n--- json3 ---\n%s", json1, json3)
	}
}

func TestGeneratedJSONLoadsAndLowers(t *testing.T) {
	doc, err := dsl.ParseDSL(sample)
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes, err := doc.JSON()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	if err := os.WriteFile(path, jsonBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	jobs, err := f.EngineJobs()
	if err != nil {
		t.Fatalf("EngineJobs: %v", err)
	}

	eng := engine.New(engine.Options{Store: engine.NewMemoryStore()})
	eng.Register("noop", func(_ context.Context, _ engine.Step) error { return nil })
	for _, j := range jobs {
		if err := eng.AddJob(j); err != nil {
			t.Fatalf("AddJob %q: %v", j.Name, err)
		}
	}

	// The parallel group should have lowered to a dependency on the prior stage.
	build, ok := eng.Job("build")
	if !ok {
		t.Fatal("build job missing")
	}
	for _, s := range build.Steps {
		if s.Name == "test-int" {
			if len(s.DependsOn) != 1 || s.DependsOn[0] != "compile" {
				t.Fatalf("test-int deps = %v, want [compile]", s.DependsOn)
			}
		}
	}
}

func TestHandlerArgQuotingRoundTrips(t *testing.T) {
	src := "job j\n  step s\n    handler log a \"b c\" d\n"
	doc, err := dsl.ParseDSL(src)
	if err != nil {
		t.Fatal(err)
	}
	args := doc.Jobs[0].Stages[0].Steps[0].Args
	want := []string{"a", "b c", "d"}
	if len(args) != 3 || args[0] != want[0] || args[1] != want[1] || args[2] != want[2] {
		t.Fatalf("args = %v, want %v", args, want)
	}
	if doc.DSL() != src {
		t.Fatalf("quoting did not round trip:\n%s", doc.DSL())
	}
}

func TestNoWarnRoundTrips(t *testing.T) {
	src := "no-warn shell-missing-flag all\n\njob j\n  step s\n    run x\n"
	doc, err := dsl.ParseDSL(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.NoWarn) != 2 || doc.NoWarn[0] != "shell-missing-flag" || doc.NoWarn[1] != "all" {
		t.Fatalf("NoWarn = %v", doc.NoWarn)
	}
	if got := doc.DSL(); got != src {
		t.Fatalf("no-warn did not round trip:\n%s", got)
	}
	j, err := doc.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(j), `"noWarn"`) {
		t.Fatalf("JSON missing noWarn:\n%s", j)
	}
}

func TestRedirectionRoundTrips(t *testing.T) {
	src := "job j\n  step s\n    run mycmd\n    stdin in.txt\n    stdout out.txt\n    stderr-append err.log\n"
	doc, err := dsl.ParseDSL(src)
	if err != nil {
		t.Fatal(err)
	}
	st := doc.Jobs[0].Stages[0].Steps[0]
	if st.Stdin != "in.txt" || st.Stdout != "out.txt" || st.StdoutAppend {
		t.Fatalf("stdin/stdout parse: %+v", st)
	}
	if st.Stderr != "err.log" || !st.StderrAppend {
		t.Fatalf("stderr-append parse: %+v", st)
	}
	if got := doc.DSL(); got != src {
		t.Fatalf("redirection did not round trip:\n%s", got)
	}
}

func TestParseErrors(t *testing.T) {
	bad := map[string]string{
		"both run and handler":    "job j\n  step s\n    run x\n    handler noop\n",
		"neither run nor handler": "job j\n  step s\n    retries 1\n",
		"empty parallel":          "job j\n  parallel\n  step s\n    run x\n",
		"unknown keyword":         "job j\n  frobnicate\n",
		"bad retries":             "job j\n  step s\n    run x\n    retries abc\n",
		"non-step in parallel":    "job j\n  parallel\n    run x\n",
	}
	for name, src := range bad {
		if _, err := dsl.ParseDSL(src); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
