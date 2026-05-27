class SchedulePassPlanner
  def plan(task_states : Array(TaskState), filter : Set(String)) : Array(SchedulePassDecision)
    decisions = [] of SchedulePassDecision
    do_filter = filter.size > 0
    task_states.each do |task_state|
      if do_filter && !filter.includes?(task_state.task.name)
        next
      end

      wait_state = task_state.should_run?
      action = if wait_state[:reason].none?
                 SchedulePassAction::StartTask
               elsif task_state.should_notify_overtime?
                 SchedulePassAction::NotifyOvertime
               else
                 SchedulePassAction::None
               end
      decisions << SchedulePassDecision.new(
        task_state: task_state,
        wait_state: wait_state,
        action: action)
    end
    decisions
  end
end

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
  @planner : SchedulePassPlanner
  @lifecycle : ScheduleTaskLifecycle
  @clock : Clock

  def initialize(@planner : SchedulePassPlanner, @lifecycle : ScheduleTaskLifecycle, @clock : Clock)
  end

  def run(task_states : Array(TaskState), filter : Set(String), chan : Channel(Time), events : Channel(TaskEvent)) : SchedulePassRunResult
    pass_time = @clock.now
    reasons = [] of TaskWaitState
    @planner.plan(task_states, filter).each do |decision|
      task_state = decision.task_state
      reason = decision.wait_state
      case decision.action
      when .start_task?
        spawn do
          task_state.run(chan, events)
        end
        sleep(0.seconds)
        @lifecycle.started(task_state, chan.receive)
      when .notify_overtime?
        @lifecycle.notify_overtime(task_state)
      when .none?
      end
      reasons << reason
    end

    SchedulePassRunResult.new(reasons: reasons, pass_time: pass_time)
  end
end

class ScheduleCompletionEvaluator
  def all_tasks_have_run_once_since?(task_states : Array(TaskState), filter : Set(String), start_time : Time) : Bool
    do_filter = filter.size > 0
    task_states.each do |task_state|
      if do_filter && !filter.includes?(task_state.task.name)
        next
      end
      return false unless task_state.has_run_successfully_once_since?(start_time)
    end
    true
  end
end
