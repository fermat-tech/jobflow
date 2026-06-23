package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fermat-tech/jobflow/engine"
)

// registerHandlers installs the built-in Go step handlers. Library users can
// register their own with eng.Register; these exist so handler steps are
// usable straight from the CLI and for testing.
func registerHandlers(eng *engine.Engine) {
	// noop: succeeds immediately.
	eng.Register("noop", func(ctx context.Context, step engine.Step) error {
		return nil
	})

	// log: prints its args (space-joined) to stdout.
	eng.Register("log", func(ctx context.Context, step engine.Step) error {
		fmt.Println(strings.Join(step.Args, " "))
		return nil
	})

	// sleep: waits for args[0] (a Go duration, e.g. "2s"), respecting context.
	eng.Register("sleep", func(ctx context.Context, step engine.Step) error {
		if len(step.Args) == 0 {
			return errors.New("sleep handler needs a duration arg")
		}
		d, err := time.ParseDuration(step.Args[0])
		if err != nil {
			return fmt.Errorf("sleep handler: %w", err)
		}
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// fail: always returns an error (args[0] is the optional message). Handy
	// for exercising retries, continueOnError, and dependency gating.
	eng.Register("fail", func(ctx context.Context, step engine.Step) error {
		msg := "intentional failure"
		if len(step.Args) > 0 {
			msg = strings.Join(step.Args, " ")
		}
		return errors.New(msg)
	})
}
