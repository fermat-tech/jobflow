package dsl

import (
	"fmt"
	"strconv"
	"strings"
)

// node is one significant line plus its indentation-nested children.
type node struct {
	indent   int
	text     string // trimmed line (no leading/trailing whitespace)
	line     int    // 1-based source line number
	children []*node
}

// ParseDSL parses jobflow DSL text into a Document.
//
// Structure is defined by indentation (spaces; tabs count as 4). Blank lines
// and lines whose first non-space character is '#' are ignored.
//
//	shell <args...>                  (optional, top level)
//	no-warn <codes...>               (optional, top level; or "all")
//	runner <name>                    (optional, top level; defines a runner)
//	  ssh <args...>                  (remote: local ssh invocation)
//	  shell <args...>                (the interpreter)
//	job <name>
//	  description <text>             (optional, free-form)
//	  every <dur> | schedule <spec>  (optional)
//	  needs <job>, <job>             (optional, job-level deps)
//	  runner <name>                  (optional, default runner for the job)
//	  step <name>                    (a single-step stage)
//	    description <text>           (optional, free-form)
//	    run <command...>             (OR) handler <name> [args...]
//	    runner <name>                (optional, command steps only)
//	    stdin <path>                 (command steps only)
//	    stdout <path>                (OR) stdout-append <path>
//	    stderr <path>                (OR) stderr-append <path>
//	    needs <step>, <step>         (optional, step-level deps)
//	    retries <n>
//	    retry-delay <dur>
//	    timeout <dur>
//	    continue-on-error
//	  parallel                       (a concurrent stage)
//	    step <name>
//	      ...
func ParseDSL(src string) (*Document, error) {
	nodes, err := lex(src)
	if err != nil {
		return nil, err
	}
	root := buildTree(nodes)

	doc := &Document{}
	shellSeen := false
	for _, n := range root.children {
		kw, rest := keyword(n.text)
		switch kw {
		case "shell":
			if shellSeen {
				return nil, lineErr(n, "duplicate 'shell' directive")
			}
			shellSeen = true
			doc.Shell = tokenize(rest)
		case "no-warn":
			doc.NoWarn = append(doc.NoWarn, tokenize(rest)...)
		case "runner":
			rn, err := parseRunner(n, rest)
			if err != nil {
				return nil, err
			}
			doc.Runners = append(doc.Runners, *rn)
		case "job":
			job, err := parseJob(n, rest)
			if err != nil {
				return nil, err
			}
			doc.Jobs = append(doc.Jobs, *job)
		default:
			return nil, lineErr(n, "unexpected %q at top level (want 'job', 'shell', 'no-warn', or 'runner')", kw)
		}
	}
	if len(doc.Jobs) == 0 {
		return nil, fmt.Errorf("dsl: no jobs defined")
	}
	return doc, doc.validate()
}

func parseJob(n *node, rest string) (*Job, error) {
	name := firstToken(rest)
	if name == "" {
		return nil, lineErr(n, "'job' requires a name")
	}
	job := &Job{Name: name}
	for _, c := range n.children {
		kw, r := keyword(c.text)
		switch kw {
		case "description":
			job.Description = r
		case "every":
			if r == "" {
				return nil, lineErr(c, "'every' requires a duration")
			}
			job.Schedule = "@every " + r
		case "schedule":
			if r == "" {
				return nil, lineErr(c, "'schedule' requires a cron spec")
			}
			job.Schedule = r
		case "needs":
			job.Needs = parseList(r)
		case "runner":
			job.Runner = firstToken(r)
		case "step":
			s, err := parseStep(c, r)
			if err != nil {
				return nil, err
			}
			job.Stages = append(job.Stages, Stage{Steps: []Step{*s}})
		case "parallel":
			stage, err := parseParallel(c)
			if err != nil {
				return nil, err
			}
			job.Stages = append(job.Stages, *stage)
		default:
			return nil, lineErr(c, "unexpected %q in job %q", kw, name)
		}
	}
	return job, nil
}

func parseRunner(n *node, rest string) (*Runner, error) {
	name := firstToken(rest)
	if name == "" {
		return nil, lineErr(n, "'runner' definition requires a name")
	}
	rn := &Runner{Name: name}
	for _, c := range n.children {
		kw, r := keyword(c.text)
		switch kw {
		case "ssh":
			rn.SSH = tokenize(r)
		case "shell":
			rn.Shell = tokenize(r)
		default:
			return nil, lineErr(c, "unexpected %q in runner %q (want 'ssh' or 'shell')", kw, name)
		}
	}
	return rn, nil
}

func parseParallel(n *node) (*Stage, error) {
	stage := &Stage{Parallel: true}
	for _, c := range n.children {
		kw, r := keyword(c.text)
		if kw != "step" {
			return nil, lineErr(c, "only 'step' is allowed inside 'parallel', got %q", kw)
		}
		s, err := parseStep(c, r)
		if err != nil {
			return nil, err
		}
		stage.Steps = append(stage.Steps, *s)
	}
	if len(stage.Steps) == 0 {
		return nil, lineErr(n, "'parallel' group has no steps")
	}
	return stage, nil
}

func parseStep(n *node, rest string) (*Step, error) {
	name := firstToken(rest)
	if name == "" {
		return nil, lineErr(n, "'step' requires a name")
	}
	s := &Step{Name: name}
	for _, c := range n.children {
		kw, r := keyword(c.text)
		switch kw {
		case "description":
			s.Description = r
		case "run":
			if r == "" {
				return nil, lineErr(c, "'run' requires a command")
			}
			s.Command = r
		case "handler":
			toks := tokenize(r)
			if len(toks) == 0 {
				return nil, lineErr(c, "'handler' requires a name")
			}
			s.Handler = toks[0]
			s.Args = toks[1:]
		case "runner":
			s.Runner = firstToken(r)
		case "stdin":
			if r == "" {
				return nil, lineErr(c, "'stdin' requires a file path")
			}
			s.Stdin = r
		case "stdout":
			if r == "" {
				return nil, lineErr(c, "'stdout' requires a file path")
			}
			s.Stdout, s.StdoutAppend = r, false
		case "stdout-append":
			if r == "" {
				return nil, lineErr(c, "'stdout-append' requires a file path")
			}
			s.Stdout, s.StdoutAppend = r, true
		case "stderr":
			if r == "" {
				return nil, lineErr(c, "'stderr' requires a file path")
			}
			s.Stderr, s.StderrAppend = r, false
		case "stderr-append":
			if r == "" {
				return nil, lineErr(c, "'stderr-append' requires a file path")
			}
			s.Stderr, s.StderrAppend = r, true
		case "needs":
			s.Needs = parseList(r)
		case "retries":
			v, err := strconv.Atoi(strings.TrimSpace(r))
			if err != nil {
				return nil, lineErr(c, "'retries' wants an integer, got %q", r)
			}
			s.Retries = v
		case "retry-delay":
			s.RetryDelay = strings.TrimSpace(r)
		case "timeout":
			s.Timeout = strings.TrimSpace(r)
		case "continue-on-error":
			if strings.TrimSpace(r) != "" {
				return nil, lineErr(c, "'continue-on-error' takes no argument")
			}
			s.ContinueOnError = true
		default:
			return nil, lineErr(c, "unexpected %q in step %q", kw, name)
		}
	}
	return s, nil
}

// lex turns source text into significant nodes, skipping blanks and comments.
func lex(src string) ([]*node, error) {
	var out []*node
	for i, raw := range strings.Split(src, "\n") {
		line := strings.TrimRight(raw, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		out = append(out, &node{indent: indentWidth(line), text: trimmed, line: i + 1})
	}
	return out, nil
}

// buildTree nests nodes by indentation under a sentinel root (indent -1).
func buildTree(nodes []*node) *node {
	root := &node{indent: -1}
	stack := []*node{root}
	for _, n := range nodes {
		for len(stack) > 1 && stack[len(stack)-1].indent >= n.indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1]
		parent.children = append(parent.children, n)
		stack = append(stack, n)
	}
	return root
}

// indentWidth counts leading whitespace, expanding tabs to 4 columns.
func indentWidth(line string) int {
	w := 0
	for _, r := range line {
		switch r {
		case ' ':
			w++
		case '\t':
			w += 4
		default:
			return w
		}
	}
	return w
}

// keyword splits a line into its first token and the trimmed remainder.
func keyword(text string) (kw, rest string) {
	if i := strings.IndexAny(text, " \t"); i >= 0 {
		return text[:i], strings.TrimSpace(text[i+1:])
	}
	return text, ""
}

func firstToken(s string) string {
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i]
	}
	return s
}

// parseList splits a comma-separated list, trimming and dropping empties.
func parseList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// tokenize splits on whitespace with double-quote grouping. Inside quotes,
// \" and \\ are unescaped.
func tokenize(s string) []string {
	var out []string
	var cur strings.Builder
	inTok, inQuote, esc := false, false, false
	flush := func() {
		if inTok {
			out = append(out, cur.String())
			cur.Reset()
			inTok = false
		}
	}
	for _, r := range s {
		switch {
		case esc:
			cur.WriteRune(r)
			esc = false
		case r == '\\' && inQuote:
			esc = true
		case r == '"':
			inQuote = !inQuote
			inTok = true // an empty "" is still a token
		case (r == ' ' || r == '\t') && !inQuote:
			flush()
		default:
			cur.WriteRune(r)
			inTok = true
		}
	}
	flush()
	return out
}

func lineErr(n *node, format string, args ...any) error {
	return fmt.Errorf("dsl: line %d: %s", n.line, fmt.Sprintf(format, args...))
}
