# Scheduler Map

This file is a fast re-orientation guide for scheduler runtime flow.

## Entry Point

- CLI wiring is in `src/crdo/cli.cr`.
- `main` creates `Schedule` and a `Channel(RunState)` for signal-driven commands.
- `Schedule#loop` delegates to `ScheduleLoopRunner`.

## Runtime Loop

Core loop code is in `src/crdo/schedule_runtime.cr`.

1. `ScheduleLoopRunner#run` loads schedule state via `Schedule#load(true)`.
2. Loop iteration asks `ScheduleLoopController#next_command`:
   - `SchedulePass`
   - `Wait`
   - `SaveAndExit`
   - `Exit`
3. If `SchedulePass`, `Schedule#run_scheduling_pass` executes one pass.
4. Waiter blocks on:
   - run-state events (if channel exists),
   - task completion events,
   - timeout.
5. Incoming event is processed via:
   - `ScheduleLoopController#handle_event` (returns a unified scheduler command)
   - `ScheduleEventActions#apply` (executes command side effects)

## Pass Path

Core pass code is in `src/crdo/schedule_task_runtime.cr`.

1. `SchedulePassPlanner#plan` evaluates each task into a pass decision:
   - start task
   - notify overtime
   - no-op
2. `SchedulePassRunner#run` executes those decisions.
3. `ScheduleTaskLifecycle` applies start/stop side effects and deferred task activation.
4. `ScheduleCompletionCheck` answers immediate-mode completion checks.

## Reload Path

Core reload/state code is in `src/crdo/schedule_reload.cr`.

1. `ScheduleReloader#load` parses and verifies crontab config.
2. `ScheduleReloadPlanner#plan` classifies each task:
   - keep current
   - retire current + defer replacement
   - replace now
3. `ScheduleReloader` builds post-reload runtime state:
   - `task_states`
   - `deferred_tasks`
   - global knobs (`autosave`, `print_report`)
4. `ScheduleDependencyState#reset` clears parent dependency flags after load/reload.
5. State persistence compatibility (`.state` legacy + v2) is also in `ScheduleReloader`.

## Signal Path

- Signal traps in `cli.cr` send run-state requests to channel:
  - `HUP` -> `Reload`
  - `INT` -> `Exit`
  - `USR1` -> `PrintReport`
  - `USR2` -> `PrintRunningReport`
- Channel is consumed by loop waiter and interpreted by `ScheduleLoopController`.
- `spec/signal_integration_spec.cr` validates this wiring end-to-end.
