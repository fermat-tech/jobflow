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
  `retryDelay`, `timeout`, `continueOnError`.
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

### Steps: sequential or parallel

Steps form a DAG *within* a job, mirroring the job-level dependency model:

- If **no** step declares `dependsOn`, the job runs **sequentially** in
  declaration order (the default — existing configs are unchanged).
- If **any** step declares `dependsOn`, the job runs as a DAG: steps with no
  declared deps start immediately and in parallel, and each step waits only for
  the steps it lists. Independent branches run concurrently; a step listing
  several deps acts as a join point.

```json
{ "name": "checkout" },
{ "name": "build-linux",   "dependsOn": ["checkout"] },
{ "name": "build-windows", "dependsOn": ["checkout"] },
{ "name": "release",       "dependsOn": ["build-linux", "build-windows"] }
```

Here the two builds run in parallel after `checkout`, and `release` runs once
both finish (see `examples/parallel.json`). If a step fails (and is not
`continueOnError`), steps depending on it are **skipped** ("a dependency did not
succeed") and the job fails. Step-dependency cycles and references to unknown
steps are rejected when the job is added.

**Restart over a DAG:** `restart <job> <step>` re-runs that step *and every step
that transitively depends on it*; all other steps are presumed done and recorded
as skipped. E.g. `restart pipeline build-windows` re-runs `build-windows` and
`release`, leaving `checkout` and the other builds untouched.

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

-config FILE   jobs config       (default "jobflow.json")
-state  FILE   persisted state    (default "jobflow-state.json")
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

Built-in handlers (for use straight from the CLI / tests): `noop`, `log`
(prints its args), `sleep` (waits `args[0]`), `fail` (always errors — useful
for exercising retries and gating).

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

```
main.go                  CLI: serve/list/status/trigger/restart/validate/handlers
handlers.go              built-in Go step handlers
internal/cron/           dependency-free cron parser + Next()
internal/engine/         Engine, jobs/steps/runs, registry, store, DAG validation
internal/config/         JSON config -> engine jobs
examples/                sample configs
```
