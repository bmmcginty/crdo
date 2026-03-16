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
