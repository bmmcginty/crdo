class SelectLoopWaiter < LoopWaiter
  def wait(run_state_channel : Channel(RunState)?, events : Channel(TaskEvent), wait_time : Time::Span) : LoopWaitResult
    if run_state_channel
      select
      when t = run_state_channel.receive
        LoopWaitResult.new(kind: LoopWaitResultKind::RunState, run_state: t, task_event: nil)
      when x = events.receive
        LoopWaitResult.new(kind: LoopWaitResultKind::TaskEvent, run_state: nil, task_event: x)
      when timeout(wait_time)
        LoopWaitResult.new(kind: LoopWaitResultKind::Timeout, run_state: nil, task_event: nil)
      end
    else
      select
      when x = events.receive
        LoopWaitResult.new(kind: LoopWaitResultKind::TaskEvent, run_state: nil, task_event: x)
      when timeout(wait_time)
        LoopWaitResult.new(kind: LoopWaitResultKind::Timeout, run_state: nil, task_event: nil)
      end
    end
  end
end
