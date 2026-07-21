# crdo Scheduler Whiteboard

## Runtime Shape

```text
CLI signals
  -> Channel(SchedulerEvent)

ScheduleRuntime
  owns the event loop
  owns task start/stop dispatch
  owns autosave timing
  owns drain/exit behavior
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
  emits TaskStoppedEvent

TaskRunEligibilityEvaluator
  decides whether one task should run from explicit context

ScheduleReporter / TaskMailer / TaskProcessRunner
  handle output, mail, and process/log IO
```

## Main Loop

```text
load initial schedule state
set next autosave time

loop
  save state if autosave is due
  update drain state

  if exit requested and no tasks are running
    save state
    return

  if normal and not draining
    run due tasks
    record wait reasons
    compute next wake timeout

  wait for one SchedulerEvent or timeout

  case event
  when Timeout
    continue
  when TaskStopped
    mark task stopped
    clear dependency requirements
    propagate success or handle failure
    retire/promote replacement tasks
    print stop report
  when ReloadRequested
    reload config and reset autosave timer
  when SaveRequested
    save state
  when PrintReportRequested
    print schedule report
  when PrintRunningReportRequested
    print running task report
  when ExitRequested
    enter draining mode
  end
end
```

## Task Start/Stop

```text
run_due_tasks
  for each task state
    build TaskRunContext
    evaluate eligibility

    if due
      mark task started
      print start report
      spawn task command fiber

    if already running and overtime
      send overtime mail

task command fiber
  run commands in order
  stop at first non-zero rc
  write command logs and rc files
  send TaskStoppedEvent
```

## Reload

```text
reload config
  parse and verify crontab
  compare incoming tasks with current task states

  unchanged task
    keep current TaskState

  changed running task
    keep current TaskState as retiring
    defer replacement Task

  changed stopped task
    replace TaskState
    preserve last run snapshot

  removed running task
    keep current TaskState as retiring

task stop after reload
  remove retiring TaskState
  promote deferred replacement with previous snapshot
```

## Boundaries

```text
ScheduleRuntime is the switchboard.
ScheduleState is mutable scheduler state.
ScheduleStore is persistence/config loading.
TaskState is one task's facts plus command execution.
Evaluator is pure scheduling policy.
Reporter/Mailer/ProcessRunner are IO edges.
```
