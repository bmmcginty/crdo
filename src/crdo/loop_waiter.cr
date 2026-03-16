abstract class LoopWaiter
  abstract def wait(run_state_channel : Channel(RunState)?, events : Channel(TaskEvent), wait_time : Time::Span) : ScheduleEvent
end
