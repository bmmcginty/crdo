record TaskRunContext,
  running : Array(TaskState),
  immediate : Bool,
  filter : Set(String),
  now : Time
