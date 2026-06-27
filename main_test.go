package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fermat-tech/jobflow/engine"
)

func TestHasFlagAndFirstNonFlag(t *testing.T) {
	args := []string{"--all", "jobA", "-ordered"}
	if !hasFlag(args, "all") {
		t.Error("--all not detected")
	}
	if !hasFlag(args, "ordered") {
		t.Error("-ordered not detected")
	}
	if hasFlag(args, "json") {
		t.Error("json should be absent")
	}
	if got := firstNonFlag(args); got != "jobA" {
		t.Errorf("firstNonFlag = %q, want jobA", got)
	}
	if firstNonFlag([]string{"--all"}) != "" {
		t.Error("firstNonFlag of only-flags should be empty")
	}
}

func newJobEngine(t *testing.T) *engine.Engine {
	t.Helper()
	eng := engine.New(engine.Options{Store: engine.NewMemoryStore()})
	for _, n := range []string{"alpha", "beta"} {
		if err := eng.AddJob(&engine.Job{Name: n, Steps: []engine.Step{{Name: "s", Command: "echo"}}}); err != nil {
			t.Fatal(err)
		}
	}
	return eng
}

func TestPrintJobNames(t *testing.T) {
	var buf bytes.Buffer
	printJobNames(&buf, newJobEngine(t))
	if got := buf.String(); got != "alpha\nbeta\n" {
		t.Fatalf("names = %q", got)
	}
}

func TestPrintJobsJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := printJobsJSON(&buf, newJobEngine(t)); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(got) != 2 || got[0]["name"] != "alpha" {
		t.Fatalf("json = %s", buf.String())
	}
}

const dslSample = `job ci
  every 1m
  step checkout
    run git pull
  parallel
    step build-linux
      run make linux
    step build-windows
      run make windows
`

func TestRunUnknownCommand(t *testing.T) {
	// An unrecognized command must report itself, not fall through to load the
	// default config file.
	err := run([]string{"-bogus"})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("err = %v, want an 'unknown command' error", err)
	}
	if err != nil && strings.Contains(err.Error(), "jobflow.json") {
		t.Fatalf("unknown command should not touch the default config: %v", err)
	}
}

func TestRunDashedSubcommand(t *testing.T) {
	// "-to-json" must be accepted as the to-json subcommand and read the file
	// given to it, rather than falling through to open jobflow.json.
	err := run([]string{"-to-json", "definitely-missing-file.jobflow"})
	if err == nil {
		t.Fatal("expected an error reading the missing file")
	}
	if strings.Contains(err.Error(), "jobflow.json") {
		t.Fatalf("dashed subcommand fell through to the default config: %v", err)
	}
	if !strings.Contains(err.Error(), "definitely-missing-file.jobflow") {
		t.Fatalf("error should reference the given file: %v", err)
	}
}

// TestResolveVersion guards against regressing to a meaningless placeholder.
func TestResolveVersion(t *testing.T) {
	if v := resolveVersion(); v == "" || v == "dev" {
		t.Fatalf("resolveVersion returned %q; want a real version or VCS-derived value", v)
	}
}

// TestConvertToJSON checks the to-json CLI path produces JSON that parses.
func TestConvertToJSON(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "p.jobflow")
	if err := os.WriteFile(src, []byte(dslSample), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := doConvert("to-json", []string{src}, &buf); err != nil {
		t.Fatalf("to-json: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "\"parallel\"") {
		t.Errorf("expected a parallel group in the JSON output:\n%s", buf.String())
	}
}

// TestConvertRoundTripThroughCLI runs DSL -> JSON -> DSL through the CLI entry
// point and confirms the DSL is reproduced.
func TestConvertRoundTripThroughCLI(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "p.jobflow")
	if err := os.WriteFile(src, []byte(dslSample), 0o644); err != nil {
		t.Fatal(err)
	}

	var jsonBuf bytes.Buffer
	if err := doConvert("to-json", []string{src}, &jsonBuf); err != nil {
		t.Fatal(err)
	}
	jsonFile := filepath.Join(dir, "p.json")
	if err := os.WriteFile(jsonFile, jsonBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	var dslBuf bytes.Buffer
	if err := doConvert("to-dsl", []string{jsonFile}, &dslBuf); err != nil {
		t.Fatal(err)
	}
	if dslBuf.String() != dslSample {
		t.Fatalf("round trip drifted.\n--- got ---\n%s\n--- want ---\n%s", dslBuf.String(), dslSample)
	}
}
