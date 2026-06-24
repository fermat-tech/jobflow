package dsl

import (
	"strconv"
	"strings"
)

// DSL renders the document as canonical DSL text (2-space indentation). The
// output re-parses to an equivalent Document, so JSON->DSL->JSON is stable.
func (d *Document) DSL() string {
	var b strings.Builder
	header := false
	if len(d.Shell) > 0 {
		b.WriteString("shell " + joinArgs(d.Shell) + "\n")
		header = true
	}
	if len(d.NoWarn) > 0 {
		b.WriteString("no-warn " + joinArgs(d.NoWarn) + "\n")
		header = true
	}
	if header && len(d.Jobs) > 0 {
		b.WriteByte('\n')
	}
	for i, job := range d.Jobs {
		if i > 0 {
			b.WriteByte('\n')
		}
		writeJob(&b, job)
	}
	return b.String()
}

func writeJob(b *strings.Builder, job Job) {
	b.WriteString("job " + job.Name + "\n")
	if job.Schedule != "" {
		if dur, ok := strings.CutPrefix(job.Schedule, "@every "); ok {
			b.WriteString("  every " + dur + "\n")
		} else {
			b.WriteString("  schedule " + job.Schedule + "\n")
		}
	}
	if len(job.Needs) > 0 {
		b.WriteString("  needs " + strings.Join(job.Needs, ", ") + "\n")
	}
	for _, st := range job.Stages {
		if st.Parallel {
			b.WriteString("  parallel\n")
			for _, s := range st.Steps {
				writeStep(b, s, "    ")
			}
		} else {
			writeStep(b, st.Steps[0], "  ")
		}
	}
}

func writeStep(b *strings.Builder, s Step, indent string) {
	body := indent + "  "
	b.WriteString(indent + "step " + s.Name + "\n")
	if s.Command != "" {
		b.WriteString(body + "run " + s.Command + "\n")
	} else {
		line := body + "handler " + s.Handler
		if len(s.Args) > 0 {
			line += " " + joinArgs(s.Args)
		}
		b.WriteString(line + "\n")
	}
	if len(s.Needs) > 0 {
		b.WriteString(body + "needs " + strings.Join(s.Needs, ", ") + "\n")
	}
	if s.Retries != 0 {
		b.WriteString(body + "retries " + strconv.Itoa(s.Retries) + "\n")
	}
	if s.RetryDelay != "" {
		b.WriteString(body + "retry-delay " + s.RetryDelay + "\n")
	}
	if s.Timeout != "" {
		b.WriteString(body + "timeout " + s.Timeout + "\n")
	}
	if s.ContinueOnError {
		b.WriteString(body + "continue-on-error\n")
	}
}

// joinArgs renders args for a shell/handler line, quoting where needed so the
// result re-tokenizes to the same slice.
func joinArgs(args []string) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = quoteArg(a)
	}
	return strings.Join(parts, " ")
}

func quoteArg(s string) string {
	if s == "" || strings.ContainsAny(s, " \t\"\\") {
		r := strings.ReplaceAll(s, "\\", "\\\\")
		r = strings.ReplaceAll(r, "\"", "\\\"")
		return "\"" + r + "\""
	}
	return s
}
