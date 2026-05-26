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
