class SelectLoopWaiter < LoopWaiter
  def wait(run_state_channel : Channel(RunState)?, events : Channel(TaskEvent), wait_time : Time::Span) : ScheduleEvent
    if run_state_channel
      select
      when t = run_state_channel.receive
        ScheduleEvent.new(kind: ScheduleEventKind::RunStateRequest, run_state: t, task_event: nil)
      when x = events.receive
        ScheduleEvent.new(kind: ScheduleEventKind::TaskCompleted, run_state: nil, task_event: x)
      when timeout(wait_time)
        ScheduleEvent.new(kind: ScheduleEventKind::Timeout, run_state: nil, task_event: nil)
      end
    else
      select
      when x = events.receive
        ScheduleEvent.new(kind: ScheduleEventKind::TaskCompleted, run_state: nil, task_event: x)
      when timeout(wait_time)
        ScheduleEvent.new(kind: ScheduleEventKind::Timeout, run_state: nil, task_event: nil)
      end
    end
  end
end
