// Command jobflow is a cron-like scheduler with multi-step jobs,
// restart-from-step, and inter-job dependencies.
//
// Usage:
//
//	jobflow [global flags] <command> [args]
//
// Commands:
//
//	serve                 run the scheduler loop (Ctrl-C to stop)
//	list                  list jobs with schedule, deps, and last status
//	status [job]          show detailed run status (all jobs, or one)
//	trigger <job>         run a job once now (ignores dependency gating)
//	restart <job> [step]  re-run a job, optionally from a step name or 1-based index
//	validate              load the config and report any errors
//	handlers              list built-in Go step handlers
//	to-json [file]        transpile DSL to JSON config (stdin/stdout)
//	to-dsl  [file]        render JSON config as DSL (stdin/stdout)
//
// Global flags:
//
//	-config string   path to the jobs config file (default "jobflow.json")
//	-state  string   path to the persisted state file (default "jobflow-state.json")
//	-no-warn string  silence warnings: "all" or comma-separated codes
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fermat-tech/jobflow/config"
	"github.com/fermat-tech/jobflow/dsl"
	"github.com/fermat-tech/jobflow/engine"
)

// version is stamped into release builds via -ldflags "-X main.version=...".
// When empty (a plain go build or `go install`), resolveVersion derives it from
// the embedded build info instead.
var version = ""

// resolveVersion returns the build version, preferring the linker-stamped value
// and otherwise falling back to Go's build info: the module version for
// `go install <module>@<ver>`, or the VCS revision for a local build.
func resolveVersion() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var rev string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev != "" {
		if len(rev) > 12 {
			rev = rev[:12]
		}
		if dirty {
			return "devel-" + rev + "+dirty"
		}
		return "devel-" + rev
	}
	return "devel"
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "jobflow: "+err.Error())
		os.Exit(1)
	}
}

func run(argv []string) error {
	// Manual flag parsing so flags may precede the subcommand.
	configPath := "jobflow.json"
	statePath := "jobflow-state.json"
	var noWarn []string
	var rest []string
	for i := 0; i < len(argv); i++ {
		switch argv[i] {
		case "-config", "--config":
			if i+1 >= len(argv) {
				return errors.New("-config needs a value")
			}
			i++
			configPath = argv[i]
		case "-state", "--state":
			if i+1 >= len(argv) {
				return errors.New("-state needs a value")
			}
			i++
			statePath = argv[i]
		case "-no-warn", "--no-warn":
			if i+1 >= len(argv) {
				return errors.New("-no-warn needs a value (\"all\" or comma-separated codes)")
			}
			i++
			for _, c := range strings.Split(argv[i], ",") {
				if c = strings.TrimSpace(c); c != "" {
					noWarn = append(noWarn, c)
				}
			}
		case "-h", "--help", "help":
			usage()
			return nil
		case "-v", "--version", "version":
			fmt.Printf("jobflow %s\n", resolveVersion())
			return nil
		default:
			rest = append(rest, argv[i])
		}
	}
	if len(rest) == 0 {
		usage()
		return nil
	}

	cmd, args := rest[0], rest[1:]
	// Accept a leading dash on subcommands (e.g. "-to-json" == "to-json"), a
	// common slip given the dashed global flags.
	cmd = strings.TrimLeft(cmd, "-")

	switch cmd {
	case "handlers":
		eng := engine.New(engine.Options{})
		registerHandlers(eng)
		fmt.Println("Built-in handlers:")
		for _, n := range eng.HandlerNames() {
			fmt.Println("  " + n)
		}
		return nil
	case "to-json", "to-dsl":
		// DSL conversions read a file (or stdin) and write to stdout; no config.
		return doConvert(cmd, args, os.Stdout)
	case "version":
		fmt.Printf("jobflow %s\n", resolveVersion())
		return nil
	case "help":
		usage()
		return nil
	case "serve", "list", "status", "trigger", "restart", "validate":
		eng, err := buildEngine(configPath, statePath, noWarn)
		if err != nil {
			return err
		}
		switch cmd {
		case "validate":
			fmt.Printf("ok: %d job(s) loaded from %s\n", len(eng.Snapshot()), configPath)
		case "list":
			printList(eng)
		case "status":
			var job string
			if len(args) > 0 {
				job = args[0]
			}
			return printStatus(eng, job)
		case "trigger":
			if len(args) < 1 {
				return errors.New("trigger needs a job name")
			}
			return doTrigger(eng, args[0])
		case "restart":
			if len(args) < 1 {
				return errors.New("restart needs a job name")
			}
			from := ""
			if len(args) > 1 {
				from = args[1]
			}
			return doRestart(eng, args[0], from)
		case "serve":
			return doServe(eng)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q (try 'jobflow help')", cmd)
	}
}

// buildEngine loads config and constructs an engine with built-in handlers and
// a persistent file store. cliNoWarn adds to any noWarn codes from the config.
func buildEngine(configPath, statePath string, cliNoWarn []string) (*engine.Engine, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	jobs, err := cfg.EngineJobs()
	if err != nil {
		return nil, err
	}
	eng := engine.New(engine.Options{
		Store:            engine.NewFileStore(statePath),
		Shell:            cfg.Shell,
		SuppressWarnings: append(append([]string(nil), cfg.NoWarn...), cliNoWarn...),
		Runners:          cfg.EngineRunners(),
	})
	registerHandlers(eng)
	for _, j := range jobs {
		if err := eng.AddJob(j); err != nil {
			return nil, err
		}
	}
	// Load persisted state so list/status/etc. reflect prior runs.
	if err := eng.LoadState(); err != nil {
		return nil, err
	}
	return eng, nil
}

// doConvert transpiles between the DSL and the JSON config format. Input comes
// from a file argument or, if absent or "-", from stdin; output is written to
// out.
func doConvert(cmd string, args []string, out io.Writer) error {
	var data []byte
	var err error
	if len(args) == 0 || args[0] == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(args[0])
	}
	if err != nil {
		return err
	}

	switch cmd {
	case "to-json":
		doc, err := dsl.ParseDSL(string(data))
		if err != nil {
			return err
		}
		j, err := doc.JSON()
		if err != nil {
			return err
		}
		_, err = out.Write(j)
		return err
	case "to-dsl":
		doc, err := dsl.FromJSON(data)
		if err != nil {
			return err
		}
		_, err = io.WriteString(out, doc.DSL())
		return err
	default:
		return fmt.Errorf("unknown convert command %q", cmd)
	}
}

func doServe(eng *engine.Engine) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fmt.Println("jobflow scheduler running. Press Ctrl-C to stop.")
	err := eng.Run(ctx)
	if errors.Is(err, context.Canceled) {
		fmt.Println("\nstopped.")
		return nil
	}
	return err
}

func doTrigger(eng *engine.Engine, job string) error {
	run, err := eng.Trigger(context.Background(), job)
	if err != nil {
		return err
	}
	printRun(run)
	if run.Status == engine.StatusFailed {
		os.Exit(1)
	}
	return nil
}

func doRestart(eng *engine.Engine, job, from string) error {
	run, err := eng.Restart(context.Background(), job, from)
	if err != nil {
		return err
	}
	printRun(run)
	if run.Status == engine.StatusFailed {
		os.Exit(1)
	}
	return nil
}

func printList(eng *engine.Engine) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tSCHEDULE\tDEPENDS ON\tLAST STATUS")
	for _, s := range eng.Snapshot() {
		sched := s.Schedule
		if sched == "" {
			sched = "-"
		}
		deps := "-"
		if len(s.DependsOn) > 0 {
			deps = strings.Join(s.DependsOn, ",")
		}
		last := "never"
		if s.Latest != nil {
			last = string(s.Latest.Status)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, sched, deps, last)
	}
	tw.Flush()
}

// printStatus prints persisted run details for all jobs, or one named job.
func printStatus(eng *engine.Engine, job string) error {
	found := false
	for _, s := range eng.Snapshot() {
		if job != "" && s.Name != job {
			continue
		}
		found = true
		fmt.Printf("== %s ==\n", s.Name)
		if s.Latest == nil {
			fmt.Println("  (never run)")
			fmt.Println()
			continue
		}
		printRun(s.Latest)
		fmt.Println()
	}
	if job != "" && !found {
		return fmt.Errorf("unknown job %q", job)
	}
	return nil
}

func printRun(r *engine.Run) {
	dur := ""
	if !r.FinishedAt.IsZero() {
		dur = " in " + r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond).String()
	}
	fmt.Printf("  run %s — %s (%s)%s\n", r.ID, r.Status, r.Trigger, dur)
	if r.Note != "" {
		fmt.Printf("  note: %s\n", r.Note)
	}
	for _, st := range r.Steps {
		marker := stepMarker(st.Status)
		line := fmt.Sprintf("    %s %s [%s]", marker, st.Name, st.Status)
		if st.Attempts > 1 {
			line += fmt.Sprintf(" (%d attempts)", st.Attempts)
		}
		if st.Error != "" {
			line += ": " + st.Error
		}
		fmt.Println(line)
	}
}

func stepMarker(s engine.Status) string {
	switch s {
	case engine.StatusSucceeded:
		return "[+]"
	case engine.StatusFailed:
		return "[x]"
	case engine.StatusSkipped:
		return "[-]"
	case engine.StatusRunning:
		return "[>]"
	default:
		return "[ ]"
	}
}

func usage() {
	fmt.Print(`jobflow — cron-like scheduler with job steps, restart, and dependencies

Usage:
  jobflow [-config FILE] [-state FILE] <command> [args]

Commands:
  serve                  run the scheduling loop until Ctrl-C
  list                   list jobs (schedule, dependencies, last status)
  status [job]           show detailed run/step status from persisted state
  trigger <job>          run a job once now (bypasses dependency gating)
  restart <job> [step]   re-run a job from the top, or from a step name/1-based index
  validate               load config and report any errors
  handlers               list built-in Go step handlers
  to-json [file]         transpile DSL (file or stdin) to JSON config on stdout
  to-dsl  [file]         render JSON config (file or stdin) as DSL on stdout
  version                print the jobflow version
  help                   show this help

Flags:
  -config FILE   jobs config (default "jobflow.json")
  -state  FILE   persisted run state (default "jobflow-state.json")
  -no-warn LIST  silence warnings: "all" or comma-separated codes
                 (e.g. -no-warn shell-missing-flag)
`)
}
