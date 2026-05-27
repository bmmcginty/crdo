record ScheduleReloadPlanEntry,
  task : Task,
  current : TaskState?,
  keep_current : Bool,
  preserve_state : Bool,
  retire_current : Bool

record ScheduleReloadPlan,
  entries : Array(ScheduleReloadPlanEntry),
  retiring_removed : Array(TaskState),
  deferred_tasks : Hash(String, Task)

enum ScheduleLoopAction
  SchedulePass
  Wait
  SaveAndExit
  Exit
end

enum ScheduleCommand
  Wait
  SchedulePass
  SaveAndExit
  Exit
  PrintReport
  PrintRunningReport
  Reload
  Save
  Transition
  Invalid
  Continue
  BreakLoop
end

record ScheduleControl,
  command : ScheduleCommand,
  requested_run_state : RunState?

enum SchedulePassAction
  StartTask
  NotifyOvertime
  None
end

record SchedulePassDecision,
  task_state : TaskState,
  wait_state : TaskWaitState,
  action : SchedulePassAction

record SchedulePassRunResult,
  reasons : Array(TaskWaitState),
  pass_time : Time

record ScheduleLoadResult,
  task_states : Array(TaskState),
  deferred_tasks : Hash(String, Task),
  autosave : Time::Span,
  print_report : Bool
