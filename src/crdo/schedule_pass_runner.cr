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
