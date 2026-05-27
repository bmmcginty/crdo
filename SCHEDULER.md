# Scheduler Map

This file is a fast re-orientation guide for scheduler runtime flow.

## Entry Point

- CLI wiring is in `src/crdo/cli.cr`.
- `main` creates `Schedule` and a `Channel(RunState)` for signal-driven commands.
- `Schedule#loop` delegates to `ScheduleLoopRunner`.

## Runtime Loop

Core loop code is in `src/crdo/schedule_runtime.cr`.

1. `ScheduleLoopRunner#run` loads schedule state via `Schedule#load(true)`.
2. Loop iteration updates local runtime state (`run_state`, `drain_state`) and decides whether scheduling is open.
3. If scheduling is open, `ScheduleLoopRunner` executes one pass directly with task runtime helpers.
4. Waiter blocks on:
   - run-state events (if channel exists),
   - task completion events,
   - timeout.
5. Incoming event is processed directly in `ScheduleLoopRunner`:
   - task completion: apply stop handling and check immediate-mode completion
   - run-state request: apply reload/save/report/exit transition logic
   - timeout: no side effects, continue loop

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
- Channel is consumed by loop waiter and handled directly in `ScheduleLoopRunner`.
- `spec/signal_integration_spec.cr` validates this wiring end-to-end.
