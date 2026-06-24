package engine

import (
	"path/filepath"
	"strings"
)

// Warning is a stable identifier for a diagnostic the engine may emit at
// startup. Codes can be silenced via Options.SuppressWarnings (or the special
// value "all").
type Warning string

const (
	// WarnShellMissingFlag fires when a custom single-element Shell names a
	// shell that needs a command-string flag (e.g. bash without "-c"), which
	// otherwise fails cryptically at run time.
	WarnShellMissingFlag Warning = "shell-missing-flag"
)

// suppressAllWarnings is the special SuppressWarnings value that silences every
// warning.
const suppressAllWarnings = "all"

// shellsNeedingFlag are shells that require a flag (e.g. -c / /C) to run a
// command string; given as a single element they will misinterpret the command
// as a script path. PowerShell/pwsh are intentionally absent — they run a bare
// command string — as are unknown shells.
var shellsNeedingFlag = map[string]bool{
	"sh": true, "bash": true, "zsh": true, "dash": true,
	"ksh": true, "posh": true, "cmd": true,
}

// shellNeedsFlag reports whether shell is a single known flag-requiring shell
// with no flag argument, returning its base name for the message.
func shellNeedsFlag(shell []string) (string, bool) {
	if len(shell) != 1 {
		return "", false
	}
	base := strings.ToLower(filepath.Base(shell[0]))
	base = strings.TrimSuffix(base, ".exe")
	if shellsNeedingFlag[base] {
		return base, true
	}
	return "", false
}

// warnf emits a warning unless its code (or "all") is suppressed. The code is
// included so users know exactly what to pass to no-warn.
func (e *Engine) warnf(code Warning, format string, args ...any) {
	if e.suppressAllWarn || e.suppressedWarn[code] {
		return
	}
	e.logf("warning [%s]: "+format, append([]any{code}, args...)...)
}

// emitStartupWarnings runs the one-time startup diagnostics.
func (e *Engine) emitStartupWarnings() {
	if _, ok := shellNeedsFlag(e.shell); ok {
		e.warnf(WarnShellMissingFlag,
			"shell %q has no command-flag argument; commands may fail (exit 127). Did you mean [%q, \"-c\"]? Suppress with -no-warn %s",
			e.shell[0], e.shell[0], WarnShellMissingFlag)
	}
}
