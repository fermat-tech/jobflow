package dagsvg_test

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/fermat-tech/jobflow/dagsvg"
	"github.com/fermat-tech/jobflow/engine"
)

func sampleJobs() []*engine.Job {
	return []*engine.Job{
		{Name: "build", Schedule: "@every 1m", Steps: []engine.Step{
			{Name: "compile", Command: "make"},
			{Name: "package", Command: "tar"},
		}},
		{Name: "test", Steps: []engine.Step{{Name: "unit", Handler: "noop"}}},
		{Name: "deploy", DependsOn: []string{"build", "test"}, Steps: []engine.Step{
			{Name: "ship", Command: "scp"},
		}},
	}
}

func wellFormed(t *testing.T, svg []byte) {
	t.Helper()
	if err := xml.Unmarshal(svg, new(struct {
		XMLName xml.Name
	})); err != nil {
		t.Fatalf("output is not well-formed XML: %v\n%s", err, svg)
	}
}

func TestRenderJobs(t *testing.T) {
	svg, err := dagsvg.Render(sampleJobs(), dagsvg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	wellFormed(t, svg)
	s := string(svg)
	if !strings.HasPrefix(s, "<svg") || !strings.HasSuffix(s, "</svg>") {
		t.Fatal("not an svg document")
	}
	for _, want := range []string{">build<", ">test<", ">deploy<", "marker-end", `class="job"`} {
		if !strings.Contains(s, want) {
			t.Errorf("svg missing %q", want)
		}
	}
	// deploy depends on build and test -> two incoming edges minimum.
	if n := strings.Count(s, "marker-end"); n < 2 {
		t.Errorf("expected >=2 edges, got %d", n)
	}
}

func TestRenderWithSteps(t *testing.T) {
	svg, err := dagsvg.Render(sampleJobs(), dagsvg.Options{Steps: true})
	if err != nil {
		t.Fatal(err)
	}
	wellFormed(t, svg)
	s := string(svg)
	for _, want := range []string{`class="cluster"`, `class="step"`, ">compile<", ">package<", ">ship<"} {
		if !strings.Contains(s, want) {
			t.Errorf("svg missing %q", want)
		}
	}
}

func TestRenderEmpty(t *testing.T) {
	if _, err := dagsvg.Render(nil, dagsvg.Options{}); err == nil {
		t.Fatal("expected error for no jobs")
	}
}

func TestEscaping(t *testing.T) {
	jobs := []*engine.Job{{Name: "a&b<c>", Steps: []engine.Step{{Name: "s", Command: "echo"}}}}
	svg, err := dagsvg.Render(jobs, dagsvg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	wellFormed(t, svg) // would fail if & / < / > were not escaped
}
