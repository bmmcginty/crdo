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

enum ScheduleEventAction
  None
  PrintReport
  PrintRunningReport
  Reload
  Save
  Transition
  Invalid
  BreakLoop
end
