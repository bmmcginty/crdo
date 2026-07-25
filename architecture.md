# crdo Architecture

This document describes how `crdo` is structured today, how work flows through the system, and how clock-shift handling works.

## High-level model

`crdo` is a single-process scheduler that:

- loads YAML config into `Task` objects
- validates task definitions and dependencies
- builds `TaskState` runtime objects
- repeatedly asks each task whether it should run now
- starts direct-exec child processes when eligible
- records task state to disk
- responds to reload, save, and report signals

```text
CLI signals
  -> Channel(SchedulerEvent)

ScheduleRuntime
  owns the event loop
  owns task start/stop dispatch
  owns autosave timing
  owns runtime mode: normal, exiting, done
  owns reload/save/report requests

ScheduleState
  owns loaded TaskState objects
  owns deferred replacements
  owns last report reasons
  owns mail failure history
  answers task lookup and task wait-state questions

ScheduleStore
  loads crontab YAML
  restores/saves .state
  plans reload keep/defer/retire behavior

TaskState
  owns mutable facts for one task
  runs task commands in a fiber
  emits a task-stopped SchedulerEvent

TaskRunEligibilityEvaluator
  decides whether one task should run from explicit context

ScheduleReporter / TaskMailer / TaskProcessRunner
  handle output, mail, and process/log IO
```

## Entry points

- `src/crdo.cr` is the thin executable entrypoint; it just calls `main`.
- `src/crdo/all.cr` requires every library file, in dependency order, without starting the program.
- `src/crdo/cli.cr` parses CLI flags, installs signal handlers, and defines `main`.

`main` builds a `ScheduleState` and hands it to `ScheduleRuntime#run`.

## Main runtime flow

The runtime lives entirely in `src/crdo/schedule_runtime.cr` (`ScheduleRuntime`). There is no separate loop-controller/loop-runner/pass-planner split; `ScheduleRuntime#run` is a single explicit loop.

```text
load initial schedule state
set next autosave time

loop
  save state if autosave is due
  update runtime mode

  if exit requested and no tasks are running
    save state
    return

  if normal
    run due tasks
    record wait reasons
    compute next wake timeout

  wait for one SchedulerEvent or timeout

  case event
  when Timeout               -> continue
  when TaskStopped           -> mark stopped, propagate deps, retire/promote, report
  when ReloadRequested       -> reload config, reset autosave timer
  when SaveRequested         -> save state
  when PrintReportRequested  -> print full schedule report
  when PrintRunningReportRequested -> print running-only report
  when ExitRequested         -> enter exiting mode, terminate running tasks
  end
end
```

Signal handlers in `cli.cr` push a `SchedulerEvent` onto a channel:

- `HUP` -> `reload_requested`
- `INT` -> `exit_requested`
- `USR1` -> `print_report_requested`
- `USR2` -> `print_running_report_requested`

`ScheduleRuntime#handle_event` consumes those events directly; there is no intermediate "decision" type between the event and the action taken.

## Config loading and validation

Config-related code is split into:

- `src/crdo/global_config.cr`: parses the `global:` section.
- `src/crdo/crontab_source_loader.cr`: loads the root YAML file and merges `include` files.
- `src/crdo/crontab.cr`: turns YAML task entries into `Task` objects.
- `src/crdo/crontab_verifier.cr`: validates commands, missing parents, and cyclical dependencies.

`Task` parsing itself lives in `src/crdo/task.cr`.

Important contract points enforced there:

- commands are array-only
- direct-exec is the default
- shell is only opt-in through explicit `/bin/sh -c ...`
- undefined `$vars` are rejected for direct-exec commands
- `every` and `when` are mutually exclusive
- `when_policy` is only valid when `when` is present

## Scheduling model

There are two scheduling modes.

### `every`

`every` uses elapsed time. It compares `now` to the task's last start time or last stop time depending on `use_stop_time`.

Code path:

- `src/crdo/task_run_eligibility_evaluator.cr`
- `src/crdo/task_state.cr`

This mode does not care about civil-time concepts like DST wall-clock folds. It only depends on elapsed durations from the scheduler's current clock.

### `when`

`when` uses wall-clock matching through `TimeMatcher` in `src/crdo/when_parser.cr`.

`parse_when` expands YAML `when` strings into one or more `TimeMatcher` objects. A matcher can constrain:

- month
- day-of-month
- weekday
- hour
- minute

Comma-separated tokens expand combinatorially within token type, so `mon,tue 23:00,07:00` becomes four matchers.

`TimeMatcher` provides:

- `match(t)`: does this wall-clock time match
- `current_slot_start?(t)`: if `t` matches, what is the start of the current slot
- `find_next(t)`: find the next matching slot after `t`
- `slot_key(t)`: local civil-time identity for a slot, used for backward clock jumps

## Task runtime objects

`Task` is the immutable config definition. `TaskState` is the mutable runtime state for a task.

`src/crdo/task_state.cr` stores:

- current run state
- last start/stop/status
- dependency state (`parent_status`)
- retirement state during reload

It delegates side effects and policy decisions to collaborators:

- `src/crdo/task_process_runner.cr`: executes task commands and writes `cron_logs/...`
- `src/crdo/task_mailer.cr`: sends overtime and failure mail
- `src/crdo/task_run_eligibility_evaluator.cr`: decides whether a task should run now or wait

There is no separate `task_stop_handler.cr`. Post-stop behavior (dependency propagation, error-command launch, failure mail, retire/promote) is handled directly by `ScheduleRuntime#handle_task_stopped` and its private helpers in `src/crdo/schedule_runtime.cr`.

## Reload and persisted state

Reload planning, state persistence, and dependency reset all live in `src/crdo/schedule_store.cr`:

- `ScheduleReloadPlanner`: pure planning — classifies each incoming task as keep/retire+defer/replace against the existing `TaskState`s
- `ScheduleStore`: loads the crontab, applies the reload plan, restores/saves `.state` (with legacy v1 array and current v2 `{version, tasks}` formats), and resets parent-dependency flags after load/reload

`src/crdo/schedule_state.cr` (`ScheduleState`) owns the resulting `Array(TaskState)` plus deferred-replacement tracking and delegates to `ScheduleStore` for the actual load/reload/save work.

Reload semantics:

- task name is the identity key
- exact normalized task signature (`Task#signature`) decides whether a task is "the same"
- unchanged running tasks stay running
- changed or removed running tasks retire and drain
- replacements for still-running changed tasks are deferred until the old task stops
- unrelated new tasks can load immediately

## Reporting

`src/crdo/schedule_reporter.cr` (`ScheduleReporter`) owns human-readable output:

- start lines
- stop lines
- full report
- running-only report
- run-state transition and invalid-transition messages

Normal operation prints start/stop lines when `global.print_report` is true. Full reports are on demand through `USR1`. Running-only reports are on demand through `USR2`.

## Clock seam

`src/crdo/clock.cr` (abstract `Clock`) and `src/crdo/system_clock.cr` (`SystemClock`) are the only testability seam for time. `Clock` lets specs inject a fake time source; there is no separate loop-waiter abstraction — `ScheduleRuntime` uses a real `select`/`timeout` directly, and specs drive it through a channel plus a fake `Clock`.

## How clock shifts are detected

Clock-shift handling matters only for `when` tasks.

`every` tasks use elapsed durations, so they do not need wall-clock DST rules. They still depend on the scheduler's chosen clock source, but they are not matched against named civil-time slots.

For `when` tasks, `crdo` detects clock jumps by comparing two scheduler timestamps tracked on `ScheduleState`:

- `previous_now`
- the `now` captured at the start of each pass

During each scheduling pass, `ScheduleRuntime#run_due_tasks`:

1. captures `pass_time = clock.now`
2. asks each task whether it should run using both `previous_now` and `pass_time` (via `TaskRunContext`)
3. calls `ScheduleState#apply_pass_result`, which stores `previous_now = pass_time` for the next pass

The evaluator uses those times like this:

### Forward jumps

Configured by `when_policy.forward`:

- `after`: if one or more matching wall-clock slots fall strictly between `previous_now` and `now`, the task runs on the next pass after the jump
- `skip`: missed slots are ignored

Implementation:

- `TaskRunEligibilityEvaluator#missed_slot_runnable?` walks candidate slots from just before `previous_now` forward until `now`
- if any candidate slot was skipped by the jump and is still runnable, the task is eligible immediately

This is how `crdo` notices a jump from, for example, `00:59` to `01:01` for a `when: 01:00` task.

### Backward jumps

Configured by `when_policy.backward`:

- `once`: a repeated wall-clock slot should only run once
- `repeat`: a repeated wall-clock slot may run again

Implementation:

- `TimeMatcher#slot_key` converts a slot to a civil-time identity such as `2026-11-01 01:00`
- `TaskRunEligibilityEvaluator#slot_runnable?` compares the current slot's `slot_key` to the previous `last_start` slot key
- if the keys are equal and policy is `once`, the repeated slot is suppressed
- if policy is `repeat`, the second occurrence is allowed

This is how `crdo` distinguishes two different absolute instants that both appear locally as `01:00` after a fall-back transition.

## Shared types

`src/crdo/types.cr` holds the shared enums and records used across the codebase: `WaitReason`, `RuntimeMode`, `SchedulerEventKind`/`SchedulerEvent`, `TaskRunDecision`, `TaskStateSnapshot`, `TaskStoppedEvent`, `WhenPolicy`, `CliOptions`, plus small parsing helpers (`parse_time_span`, `parse_when_policy`, `format_time_span`, `yaml_string_array`).

`TaskRunContext` (the small record passed into `TaskRunEligibilityEvaluator#evaluate`) is defined directly in `src/crdo/task_run_eligibility_evaluator.cr`. `ScheduleReloadPlanEntry`, `ScheduleReloadPlan`, and `ScheduleLoadResult` are defined directly in `src/crdo/schedule_store.cr`. There are no separate `task_support.cr` / `schedule_support.cr` files — each support record lives next to the code that uses it.

## Testing structure

Specs live under `spec/`. Important coverage areas:

- CLI parsing
- config loading and task validation
- `when` parsing and expansion
- state store compatibility and atomic load behavior
- reload planning
- run eligibility rules
- process runner and mailer behavior
- fake-clock `every` behavior
- loop timeout requests
- `when_policy` clock-jump handling
- signal wiring (`spec/signal_integration_spec.cr`)

The scheduler is split into `ScheduleRuntime` / `ScheduleState` / `ScheduleStore` / `TaskState` / `TaskRunEligibilityEvaluator` / `ScheduleReporter` / `TaskMailer` / `TaskProcessRunner` mainly so evaluator and store logic can be unit tested with a fake `Clock`, without needing to run the full scheduler loop for every rule.
