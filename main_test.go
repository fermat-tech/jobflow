package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
