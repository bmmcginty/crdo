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

record ScheduleLoadResult,
  states : Array(TaskState),
  deferred_tasks : Hash(String, Task),
  autosave : Time::Span,
  print_report : Bool
