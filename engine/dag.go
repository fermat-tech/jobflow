package engine

import (
	"fmt"
	"sort"
	"strings"
)

// validateGraph checks that every dependency refers to a known job and that
// the DependsOn relation contains no cycles. jobs is keyed by job name.
func validateGraph(jobs map[string]*Job) error {
	// All dependencies must resolve.
	for _, j := range jobs {
		for _, dep := range j.DependsOn {
			if dep == j.Name {
				return fmt.Errorf("job %q depends on itself", j.Name)
			}
			if _, ok := jobs[dep]; !ok {
				return fmt.Errorf("job %q depends on unknown job %q", j.Name, dep)
			}
		}
	}

	// Cycle detection via DFS with coloring (0=unvisited,1=on-stack,2=done).
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(jobs))
	var stack []string

	var visit func(name string) error
	visit = func(name string) error {
		color[name] = gray
		stack = append(stack, name)
		deps := append([]string(nil), jobs[name].DependsOn...)
		sort.Strings(deps) // deterministic error messages
		for _, dep := range deps {
			switch color[dep] {
			case gray:
				return fmt.Errorf("dependency cycle: %s -> %s", strings.Join(stack, " -> "), dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		return nil
	}

	names := make([]string, 0, len(jobs))
	for n := range jobs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if color[n] == white {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

// detectCycle reports the first dependency cycle in a generic dependency map
// (node -> nodes it depends on). label names the entity for the error message.
func detectCycle(deps map[string][]string, label string) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(deps))
	var stack []string

	var visit func(name string) error
	visit = func(name string) error {
		color[name] = gray
		stack = append(stack, name)
		ds := append([]string(nil), deps[name]...)
		sort.Strings(ds)
		for _, d := range ds {
			switch color[d] {
			case gray:
				return fmt.Errorf("%s cycle: %s -> %s", label, strings.Join(stack, " -> "), d)
			case white:
				if err := visit(d); err != nil {
					return err
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		return nil
	}

	names := make([]string, 0, len(deps))
	for n := range deps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if color[n] == white {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

// dependents returns the names of jobs that list target in their DependsOn,
// sorted for determinism.
func dependents(jobs map[string]*Job, target string) []string {
	var out []string
	for _, j := range jobs {
		for _, dep := range j.DependsOn {
			if dep == target {
				out = append(out, j.Name)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}
