# jobflow

A cron-like job scheduler with **multi-step jobs**, **restart from the top or a
specific step**, and **inter-job dependencies** (e.g. *job C runs only after A
and B both succeed*). Usable as an embeddable Go library or via the `jobflow`
CLI. No third-party dependencies.

## Build

```powershell
cd jobflow
go build -o jobflow.exe .     # CLI
go test ./...                 # tests
```

## Concepts

- **Job** — a named unit with an optional cron `schedule`, optional
  `dependsOn` list, and an ordered list of **steps**.
- **Step** — one piece of work. Exactly one of:
  - `command` — a shell command line (run via `cmd /C` on Windows,
    `/bin/sh -c` elsewhere; override with the config's `shell`).
  - `handler` — a named Go function registered with the engine.
  Per-step options: `dependsOn` (other steps in the same job), `retries`,
  `retryDelay`, `timeout`, `continueOnError`, `runner` (interpreter / remote
  target, see below), and stream redirection (`stdin`/`stdout`/`stderr`).
- **Run** — one execution of a job, recording each step's status, attempt
  count, and error. The latest run per job is persisted to disk.

### Triggering & dependency rules

A job runs when any of its triggers fire, and the engine decides as follows:

| Job has...                | Behavior                                                              |
|---------------------------|----------------------------------------------------------------------|
| `schedule` only           | runs on its cron cadence                                             |
| `dependsOn` only          | **cascade**: runs automatically when its dependencies all succeed   |
| both `schedule` + `dependsOn` | runs on schedule, but only if dependencies most-recently succeeded (otherwise the run is *skipped* and recorded) |
| neither                   | runs only on manual `trigger`/`restart`                              |

"Dependencies succeeded" means each dependency's **latest** run finished
`succeeded`. `trigger` and `restart` are explicit manual actions and bypass
dependency gating. The dependency graph is validated on load — unknown
dependencies and cycles are rejected.

> Cascade is a scheduler (`serve`) behavior. The one-shot `trigger` command
> runs a single job and does not cascade to dependents.

### Steps: sequential and parallel (stages)

A job's `steps` is an ordered list of **stages**. A stage is either a single
step or a **parallel group**. Stages run in order; steps inside a group run
concurrently, and the next stage waits for all of them. Sequential is the
default — you only reach for a group where you actually want fan-out, with no
per-step wiring:

```json
"steps": [
  { "name": "checkout", "command": "git pull" },
  { "parallel": [
      { "name": "build-linux",   "command": "make linux" },
      { "name": "build-windows", "command": "make windows" },
      { "name": "build-mac",     "command": "make mac" }
  ]},
  { "name": "release", "command": "make release" }
]
```

The three builds run in parallel after `checkout`; `release` runs once all of
them finish (see `examples/parallel.json`). If a step fails (and is not
`continueOnError`), steps depending on it are **skipped** ("a dependency did not
succeed") and the job fails.

**Advanced — arbitrary DAGs:** for asymmetric dependencies that stages can't
express (e.g. a step that depends on only *one* member of a prior group), a step
may set `dependsOn` naming other steps in the same job directly. Stage groups
are simply lowered onto this same mechanism, so the two styles compose; a group
member's explicit `dependsOn` is unioned with its stage dependency.

Step-dependency cycles and references to unknown steps are rejected when the job
is added.

**Restart over the graph:** `restart <job> <step>` re-runs that step *and every
step that transitively depends on it*; all other steps are presumed done and
recorded as skipped. E.g. `restart pipeline build-windows` re-runs
`build-windows` and `release`, leaving `checkout` and the other builds
untouched.

## CLI

```
jobflow [-config FILE] [-state FILE] <command> [args]

serve                  run the scheduling loop until Ctrl-C
list                   list jobs (schedule, dependencies, last status)
status [job]           detailed run/step status from persisted state
trigger <job>          run a job once now (bypasses dependency gating)
restart <job> [step]   re-run from the top, or from a step name / 1-based index
validate               load config and report any errors
handlers               list built-in Go step handlers
to-json [file]         transpile DSL to JSON config (stdin/stdout)
to-dsl  [file]         render JSON config as DSL (stdin/stdout)

-config FILE   jobs config       (default "jobflow.json")
-state  FILE   persisted state    (default "jobflow-state.json")
-no-warn LIST  silence warnings: "all" or comma-separated codes
```

Examples:

```powershell
jobflow -config examples/jobs.json serve
jobflow -config examples/jobs.json list
jobflow -config examples/jobs.json trigger deploy
jobflow -config examples/jobs.json restart deploy smoke   # resume from the "smoke" step
jobflow -config examples/jobs.json restart deploy 2       # resume from step #2
```

### Cron syntax

Standard 5 fields — `minute hour day-of-month month day-of-week` — supporting
`*`, single values, ranges (`1-5`), lists (`1,3,5`), and steps (`*/15`,
`1-30/5`). Month/day-of-week accept names (`jan`, `mon`). Macros: `@yearly`,
`@monthly`, `@weekly`, `@daily`, `@hourly`, and `@every <duration>` (e.g.
`@every 30s`, `@every 1h30m`). When both day-of-month and day-of-week are
restricted, a day matches if **either** matches (standard cron behavior).

## Config format

```json
{
  "shell": ["pwsh", "-NoProfile", "-Command"],
  "jobs": [
    {
      "name": "build",
      "schedule": "@every 1m",
      "steps": [
        { "name": "compile", "handler": "log", "args": ["compiling"] },
        { "name": "package", "command": "echo packaged" }
      ]
    },
    {
      "name": "test",
      "schedule": "@every 1m",
      "steps": [{ "name": "unit", "handler": "noop" }]
    },
    {
      "name": "deploy",
      "dependsOn": ["build", "test"],
      "steps": [
        { "name": "ship",  "handler": "log", "args": ["shipping"] },
        { "name": "smoke", "handler": "noop", "retries": 2, "retryDelay": "1s" }
      ]
    }
  ]
}
```

`shell` is optional. Step `retryDelay` and `timeout` are Go duration strings.

### Runners (interpreters & remote execution)

A command step runs through a **runner** — by default the engine's local
`shell`. Define named runners to use a different interpreter, or to run on a
remote host over SSH, then select one per job or per step:

```json
"runners": {
  "prod": { "ssh": ["ssh", "deploy@prod"], "shell": ["/bin/bash", "-c"] },
  "ps":   { "shell": ["pwsh", "-NoProfile", "-Command"] }
},
"jobs": [
  { "name": "deploy", "runner": "prod", "steps": [ { "name": "ship", "command": "make release" } ] },
  { "name": "mixed", "steps": [
      { "name": "local",  "command": "echo here" },
      { "name": "remote", "command": "df -h", "runner": "prod" }
  ]}
]
```

- A runner with `ssh` runs the command on that host via your local OpenSSH
  client (no extra dependency); jobflow single-quotes the command for the remote
  `shell` (default `/bin/sh -c`), so quoting and operators survive the trip. A
  runner with only `shell` is a local interpreter override.
- Resolution is **step `runner` → job `runner` → default (`shell`)**.
- SSH auth, hosts, and ports come from your `ssh` config / keys / agent —
  jobflow doesn't handle passwords. Put flags in the `ssh` array
  (`["ssh", "-p", "2222", "user@host"]`).
- Runners apply to **command steps only** (handlers run in-process and can't go
  remote — a runner on a handler step is rejected). Stream redirection still
  works: remote output comes back into your local `stdout`/`stderr` files.
- DSL: define with a `runner <name>` block (`ssh …` / `shell …` lines); select
  with a `runner <name>` line in a job or step.

### Stream redirection

A command step can redirect its standard streams to files via config, so the
command string needs no shell redirection operators (and no quoting headaches):

```json
{ "name": "report", "command": "echo This is a test",
  "stdout": "out/report.txt" },
{ "name": "log", "command": "mytool",
  "stdin": "in.txt", "stdout": "run.log", "stdoutAppend": true,
  "stderr": "run.log" }
```

`stdin` is a path opened for reading. `stdout`/`stderr` write to a file —
truncating by default, or appending when `stdoutAppend`/`stderrAppend` is true.
Pointing `stdout` and `stderr` at the same path merges them into one handle
(like `> f 2>&1`). Empty fields inherit (stdin = null device; stdout/stderr =
the engine's writers). Redirection applies to command steps only — it's
rejected on handler steps. In the DSL: `stdin <path>`, `stdout <path>` /
`stdout-append <path>`, `stderr <path>` / `stderr-append <path>`.

A custom `shell` must include the flag that runs a command string — `-c` for
sh/bash/zsh/posh, `/C` for cmd, `-Command` for PowerShell. A one-element shell
like `["bash"]` makes the shell treat the whole command as a *script filename*
(failing with exit 127). jobflow warns about this at startup:

```
warning [shell-missing-flag]: shell "bash" has no command-flag argument ...
```

Silence warnings with `-no-warn <code>` / `-no-warn all` on the CLI, or
`"noWarn": ["shell-missing-flag"]` / `["all"]` in the config (DSL: `no-warn
shell-missing-flag`). PowerShell, pwsh, and unrecognized shells are never
warned about (they accept a bare command string).

Built-in handlers (for use straight from the CLI / tests): `noop`, `log`
(prints its args), `sleep` (waits `args[0]`), `fail` (always errors — useful
for exercising retries and gating).

## DSL (friendlier than JSON)

Hand-writing JSON is tedious. The `jobflow` DSL is an indentation-based
equivalent that transpiles to the JSON config above. `run` and `handler` lines
take their arguments verbatim — no quoting, commas, or braces:

```
job ci
  every 1m
  step checkout
    run git pull
  parallel
    step build-linux
      run make linux
    step build-windows
      run make windows
  step release
    handler log shipping release

job deploy
  needs ci
  step ship
    handler noop
    retries 2
    retry-delay 5s
```

Convert in either direction (file argument or stdin; output to stdout):

```powershell
jobflow to-json pipeline.jobflow > jobs.json   # DSL  -> JSON
jobflow to-dsl  jobs.json                       # JSON -> DSL (for display)
```

Keywords: `shell` / `no-warn` / `runner` (top-level block with `ssh`/`shell`,
or a job/step reference) / `job` / `every` | `schedule` / `needs` (job- or
step-level deps) / `step` / `parallel` / `run` | `handler` / `stdin` /
`stdout` | `stdout-append` / `stderr` | `stderr-append` / `retries` /
`retry-delay` / `timeout` / `continue-on-error`. Lines beginning with `#` are
comments. JSON↔DSL round-trips preserve structure exactly; comments are not
retained (JSON has no comment syntax). The same conversion is available as a
library via package `dsl` (`ParseDSL`, `Document.JSON`, `FromJSON`,
`Document.DSL`).

## Library use

```go
eng := engine.New(engine.Options{
    Store: engine.NewFileStore("state.json"),
})

eng.Register("reindex", func(ctx context.Context, s engine.Step) error {
    return search.Reindex(ctx)
})

eng.AddJob(&engine.Job{
    Name:     "nightly",
    Schedule: "0 2 * * *",
    Steps: []engine.Step{
        {Name: "snapshot", Command: "pg_dump mydb > /backups/db.sql"},
        {Name: "reindex",  Handler: "reindex", Retries: 2, RetryDelay: 30 * time.Second},
    },
})

eng.AddJob(&engine.Job{
    Name:      "report",
    DependsOn: []string{"nightly"}, // cascades after nightly succeeds
    Steps:     []engine.Step{{Name: "email", Handler: "report"}},
})

eng.Run(ctx) // blocks until ctx is cancelled; waits for in-flight runs
```

`Trigger(ctx, name)` and `Restart(ctx, name, fromStep)` run synchronously and
return the completed `*Run`. State is persisted incrementally, so a restarted
scheduler resumes with full knowledge of past outcomes.

## Package layout

The CLI is a thin consumer of three public, importable packages:

```
main.go      CLI: serve/list/status/trigger/restart/validate/handlers
handlers.go  built-in Go step handlers (CLI only)
engine/      Engine, jobs/steps/runs, registry, store, DAG validation
cron/        dependency-free cron parser + Next()
config/      JSON config -> engine jobs
dsl/         indentation DSL <-> JSON config
examples/    sample configs (.json and .jobflow)
```

Import paths: `github.com/fermat-tech/jobflow/{engine,cron,config,dsl}`.
