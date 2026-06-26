package engine

import "strings"

// Runner names an interpreter and/or transport for command steps. When SSH is
// non-empty the command runs on a remote host via the local ssh client;
// otherwise it runs locally with Shell as the interpreter. Jobs and steps
// select a runner by name (Job.Runner / Step.Runner); the default is the
// engine's local shell.
type Runner struct {
	// Name is the key jobs and steps reference.
	Name string

	// SSH, when non-empty, is the local ssh invocation including destination
	// and flags, e.g. ["ssh", "deploy@prod"] or ["ssh", "-p", "2222", "u@h"].
	SSH []string

	// Shell is the interpreter that runs the command string. For a local
	// runner it overrides the engine's default shell; for an SSH runner it is
	// the remote interpreter. Defaults to the engine shell (local) or
	// ["/bin/sh", "-c"] (remote).
	Shell []string
}

// invocation builds the local exec name and args needed to run command through
// this runner. localDefault is the engine's default shell, used when a local
// runner specifies no Shell of its own.
//
// For an SSH runner the command is wrapped as "<remote-shell> -c '<command>'"
// and single-quoted so the remote login shell passes it to the chosen remote
// interpreter intact (this assumes a POSIX remote login shell).
func (r *Runner) invocation(command string, localDefault []string) (name string, args []string) {
	if len(r.SSH) > 0 {
		remoteShell := r.Shell
		if len(remoteShell) == 0 {
			remoteShell = []string{"/bin/sh", "-c"}
		}
		remote := strings.Join(remoteShell, " ") + " " + posixQuote(command)
		args = append(append([]string(nil), r.SSH[1:]...), remote)
		return r.SSH[0], args
	}

	sh := r.Shell
	if len(sh) == 0 {
		sh = localDefault
	}
	args = append(append([]string(nil), sh[1:]...), command)
	return sh[0], args
}

// posixQuote wraps s in single quotes for a POSIX shell, escaping embedded
// single quotes as the usual '\” sequence.
func posixQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
