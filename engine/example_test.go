package engine_test

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/fermat-tech/jobflow/engine"
)

// Example builds an engine, registers a Go handler, defines two jobs where the
// second depends on the first, and runs them. It shows the core library API a
// consumer would use to embed the scheduler in their own program.
func Example() {
	eng := engine.New(engine.Options{
		Store:  engine.NewMemoryStore(),
		Logger: log.New(io.Discard, "", 0), // silence scheduler logging for the example
	})

	eng.Register("say", func(ctx context.Context, s engine.Step) error {
		fmt.Println("ran", s.Name)
		return nil
	})

	// A two-step job. With no step-level DependsOn the steps run sequentially.
	_ = eng.AddJob(&engine.Job{
		Name: "build",
		Steps: []engine.Step{
			{Name: "compile", Handler: "say"},
			{Name: "package", Handler: "say"},
		},
	})

	run, err := eng.Trigger(context.Background(), "build")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("build:", run.Status)

	// Restart just the "package" step (and anything depending on it).
	restarted, _ := eng.Restart(context.Background(), "build", "package")
	fmt.Println("restart:", restarted.Status, "from", restarted.FromStep)

	// Output:
	// ran compile
	// ran package
	// build: succeeded
	// ran package
	// restart: succeeded from package
}
