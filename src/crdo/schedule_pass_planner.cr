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
