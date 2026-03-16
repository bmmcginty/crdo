record TaskRunContext,
  running : Array(TaskState),
  immediate : Bool,
  filter : Set(String),
  previous_now : Time?,
  now : Time
