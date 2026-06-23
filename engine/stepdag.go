package engine

// effectiveStepDeps returns the dependency list per step name for a job.
//
// If any step declares DependsOn, the job is in explicit DAG mode and each
// step's declared deps are used verbatim (steps with none start immediately).
// Otherwise linear deps are synthesized — step i depends on step i-1 — which
// reproduces sequential execution through the same DAG executor.
func effectiveStepDeps(job *Job) map[string][]string {
	explicit := false
	for _, s := range job.Steps {
		if len(s.DependsOn) > 0 {
			explicit = true
			break
		}
	}

	deps := make(map[string][]string, len(job.Steps))
	if explicit {
		for _, s := range job.Steps {
			deps[s.Name] = s.DependsOn
		}
		return deps
	}
	for i, s := range job.Steps {
		if i == 0 {
			deps[s.Name] = nil
		} else {
			deps[s.Name] = []string{job.Steps[i-1].Name}
		}
	}
	return deps
}

// transitiveStepClosure returns start plus every step that (transitively)
// depends on it. This is the set re-executed when restarting from start;
// everything else is presumed already done.
func transitiveStepClosure(deps map[string][]string, start string) map[string]bool {
	rev := make(map[string][]string)
	for s, ds := range deps {
		for _, d := range ds {
			rev[d] = append(rev[d], s)
		}
	}
	set := map[string]bool{start: true}
	stack := []string{start}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, c := range rev[n] {
			if !set[c] {
				set[c] = true
				stack = append(stack, c)
			}
		}
	}
	return set
}
