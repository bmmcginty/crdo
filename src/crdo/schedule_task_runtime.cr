class ScheduleTaskLifecycle
  @schedule : Schedule
  @reporter : ScheduleReporter

  def initialize(@schedule : Schedule, @reporter : ScheduleReporter)
  end

  def notify_overtime(task_state : TaskState)
    task_state.notify_overtime
  end

  def started(task_state : TaskState, start_time : Time)
    task_state.started(start_time)
    @reporter.started(task_state, start_time)
  end

  def stopped(event : TaskEvent)
    task_state = event[0]
    task_state.stopped(status: event[1], last_command_index: event[2], stop_time: event[3])
    @reporter.stopped(task_state, event[1], @schedule.next_task_wait(task_state))
    if task_state.retiring && !task_state.running?
      @schedule.remove_task(task_state)
      @schedule.activate_deferred_task(task_state.task.name, task_state.state_snapshot)
    elsif @schedule.deferred_task?(task_state.task.name)
      @schedule.activate_deferred_task(task_state.task.name, task_state.state_snapshot)
    end
  end
end

class SchedulePassRunner
  @lifecycle : ScheduleTaskLifecycle
  @clock : Clock

  def initialize(@lifecycle : ScheduleTaskLifecycle, @clock : Clock)
  end

  def run(task_states : Array(TaskState), filter : Set(String), chan : Channel(Time), events : Channel(TaskEvent)) : SchedulePassRunResult
    pass_time = @clock.now
    reasons = [] of TaskWaitState
    do_filter = filter.size > 0
    task_states.each do |task_state|
      if do_filter && !filter.includes?(task_state.task.name)
        next
      end

      reason = task_state.should_run?
      if reason[:reason].none?
        spawn do
          task_state.run(chan, events)
        end
        sleep(0.seconds)
        @lifecycle.started(task_state, chan.receive)
      elsif task_state.should_notify_overtime?
        @lifecycle.notify_overtime(task_state)
      end
      reasons << reason
    end

    SchedulePassRunResult.new(reasons: reasons, pass_time: pass_time)
  end
end
