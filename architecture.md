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

The program is split into small components, but the control flow is still centered on `Schedule`.

## Entry points

- `[crdo.cr](/home/bmmcginty/git/crdo/crdo.cr)` is the thin executable entrypoint.
- `[src/crdo/all.cr](/home/bmmcginty/git/crdo/src/crdo/all.cr)` loads the library code without starting the program.
- `[src/crdo/cli.cr](/home/bmmcginty/git/crdo/src/crdo/cli.cr)` parses CLI flags and installs signal handlers.

`main` creates a `Schedule` and calls `Schedule#loop`.

## Main runtime flow

The core runtime is now split between `[src/crdo/schedule.cr](/home/bmmcginty/git/crdo/src/crdo/schedule.cr)` and `[src/crdo/schedule_loop_runner.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_loop_runner.cr)`.

Startup sequence:

1. `Schedule#load(true)` builds a `Crontab`, verifies it, creates `TaskState` objects, and optionally restores persisted state.
2. `Schedule#loop` delegates to `ScheduleLoopRunner`.
3. `ScheduleLoopRunner#run` creates channels for run-state requests and task completion events.
4. Each loop iteration asks `ScheduleLoopController#next_action` what the loop should do next: run a scheduling pass, wait, save-and-exit, or exit.
5. If a scheduling pass is requested, `SchedulePassPlanner` turns each eligible `TaskState` into a `SchedulePassDecision`.
6. `Schedule` applies those planned pass actions by starting tasks or sending overtime mail, then updates wait reasons and timeout data in `ScheduleLoopController`.
7. The loop blocks in `LoopWaiter#wait` until one of three things happens:
   - a signal-driven run-state request arrives
   - a task finishes
   - the shortest computed timeout expires
8. The resulting `ScheduleEvent` is handed back to `ScheduleLoopController`, which returns a `ScheduleEventDecision` describing the higher-level action such as reload, report, save, transition, or break-loop.

## Config loading and validation

Config-related code is split into:

- `[src/crdo/global_config.cr](/home/bmmcginty/git/crdo/src/crdo/global_config.cr)`:
  parses the `global:` section.
- `[src/crdo/crontab_source_loader.cr](/home/bmmcginty/git/crdo/src/crdo/crontab_source_loader.cr)`:
  loads the root YAML file and merges `include` files.
- `[src/crdo/crontab.cr](/home/bmmcginty/git/crdo/src/crdo/crontab.cr)`:
  turns YAML task entries into `Task` objects.
- `[src/crdo/crontab_verifier.cr](/home/bmmcginty/git/crdo/src/crdo/crontab_verifier.cr)`:
  validates commands, missing parents, and cyclical dependencies.

`Task` parsing itself lives in `[src/crdo/task.cr](/home/bmmcginty/git/crdo/src/crdo/task.cr)`.

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

- `[src/crdo/task_run_eligibility_evaluator.cr](/home/bmmcginty/git/crdo/src/crdo/task_run_eligibility_evaluator.cr)`
- `[src/crdo/task_state.cr](/home/bmmcginty/git/crdo/src/crdo/task_state.cr)`

This mode does not care about civil-time concepts like DST wall-clock folds. It only depends on elapsed durations from the scheduler's current clock.

### `when`

`when` uses wall-clock matching through `TimeMatcher` in `[src/crdo/when_parser.cr](/home/bmmcginty/git/crdo/src/crdo/when_parser.cr)`.

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

`[src/crdo/task_state.cr](/home/bmmcginty/git/crdo/src/crdo/task_state.cr)` stores:

- current run state
- last start/stop/status
- dependency state
- retirement state during reload

It delegates side effects and policy decisions to collaborators:

- `[src/crdo/task_process_runner.cr](/home/bmmcginty/git/crdo/src/crdo/task_process_runner.cr)`:
  executes task commands and writes `cron_logs/...`
- `[src/crdo/task_mailer.cr](/home/bmmcginty/git/crdo/src/crdo/task_mailer.cr)`:
  sends overtime and failure mail
- `[src/crdo/task_run_eligibility_evaluator.cr](/home/bmmcginty/git/crdo/src/crdo/task_run_eligibility_evaluator.cr)`:
  decides whether a task should run now or wait
- `[src/crdo/task_stop_handler.cr](/home/bmmcginty/git/crdo/src/crdo/task_stop_handler.cr)`:
  applies post-stop behavior such as dependency propagation, error-command launch, and failure mail

The small support record used by the evaluator lives in `[src/crdo/task_support.cr](/home/bmmcginty/git/crdo/src/crdo/task_support.cr)`.

## Reload and persisted state

Reload, pass planning, and state logic are split out of `Schedule`:

- `[src/crdo/schedule_state_store.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_state_store.cr)`:
  reads legacy and v2 state, writes v2 state atomically via temp file + rename
- `[src/crdo/schedule_reload_planner.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_reload_planner.cr)`:
  computes which tasks are kept, retired, replaced, or deferred during reload
- `[src/crdo/schedule_reloader.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_reloader.cr)`:
  loads and verifies crontab config, then applies initial-load or reload task-state updates from planner results
- `[src/crdo/schedule_dependency_resetter.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_dependency_resetter.cr)`:
  clears parent dependency flags across runtime task state after load/reload
- `[src/crdo/schedule_completion_evaluator.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_completion_evaluator.cr)`:
  evaluates immediate-mode completion criteria across task state and active filter
- `[src/crdo/schedule_pass_planner.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_pass_planner.cr)`:
  turns task runtime state into explicit scheduling-pass decisions
- `[src/crdo/schedule_pass_runner.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_pass_runner.cr)`:
  executes scheduling-pass decisions and returns pass-time and wait-reason results
- `[src/crdo/schedule_task_lifecycle.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_task_lifecycle.cr)`:
  applies task start, stop, overtime, and deferred-activation side effects
- `[src/crdo/schedule_event_applier.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_event_applier.cr)`:
  applies event-decision side effects such as reload, save, report, and transition reporting
- `[src/crdo/schedule_support.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_support.cr)`:
  contains the shared plan, loop-action, event-action, and pass-decision types

Reload semantics:

- task name is the identity key
- exact normalized task signature decides whether a task is "the same"
- unchanged running tasks stay running
- changed or removed running tasks retire and drain
- replacements for still-running changed tasks are deferred until the old task stops
- unrelated new tasks can load immediately

## Reporting

`[src/crdo/schedule_reporter.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_reporter.cr)` owns human-readable output:

- start lines
- stop lines
- full report
- running-only report
- run-state transition messages

Normal operation prints start/stop lines. Full reports are on demand through `USR1`. Running-only reports are on demand through `USR2`.

## Clock and waiting seams

Two explicit seams exist for testability:

- `[src/crdo/clock.cr](/home/bmmcginty/git/crdo/src/crdo/clock.cr)` and `[src/crdo/system_clock.cr](/home/bmmcginty/git/crdo/src/crdo/system_clock.cr)`
- `[src/crdo/loop_waiter.cr](/home/bmmcginty/git/crdo/src/crdo/loop_waiter.cr)` and `[src/crdo/select_loop_waiter.cr](/home/bmmcginty/git/crdo/src/crdo/select_loop_waiter.cr)`

`Clock` lets specs inject a fake time source. `LoopWaiter` lets specs observe requested wait durations without relying on real `select` sleeps.

The loop now also has two explicit decision seams:

- `ScheduleLoopController`:
  translates scheduler state plus incoming `ScheduleEvent`s into high-level loop actions
- `SchedulePassPlanner`:
  translates current `TaskState`s into a list of concrete scheduling-pass decisions

## How clock shifts are detected

Clock-shift handling matters only for `when` tasks.

`every` tasks use elapsed durations, so they do not need wall-clock DST rules. They still depend on the scheduler's chosen clock source, but they are not matched against named civil-time slots.

For `when` tasks, `crdo` detects clock jumps by comparing two scheduler timestamps:

- `Schedule#previous_now`
- `Schedule#current_now`

During each scheduling pass, `Schedule`:

1. captures `current_now = clock.now`
2. asks each task whether it should run using both `previous_now` and `current_now`
3. stores `previous_now = current_now` at the end of the pass

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

## Type/support files

Several small shared records and enums live outside the main classes:

- `[src/crdo/types.cr](/home/bmmcginty/git/crdo/src/crdo/types.cr)`:
  shared enums, snapshots, CLI options, `WhenPolicy`, and `ScheduleEvent`
- `[src/crdo/task_support.cr](/home/bmmcginty/git/crdo/src/crdo/task_support.cr)`:
  `TaskRunContext`
- `[src/crdo/schedule_support.cr](/home/bmmcginty/git/crdo/src/crdo/schedule_support.cr)`:
  reload-plan records plus scheduler loop/pass action types

## Testing structure

Specs live under `[spec/](/home/bmmcginty/git/crdo/spec)`.

Important coverage areas:

- CLI parsing
- config loading and task validation
- `when` parsing and expansion
- state store compatibility and atomic load behavior
- reload planning
- loop controller logic
- run eligibility rules
- stop handling
- process runner and mailer behavior
- fake-clock `every` behavior
- loop timeout requests
- `when_policy` clock-jump handling

The main reason the current architecture is split this way is to make those pieces testable without needing to run the full scheduler process for every rule.
