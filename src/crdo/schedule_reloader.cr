class ScheduleReloader
  @schedule : Schedule
  @crontab_path : String

  def initialize(@schedule : Schedule, @crontab_path : String)
  end

  def load(initial : Bool, current_task_states : Array(TaskState)) : ScheduleLoadResult
    crontab = Crontab.new(@crontab_path)
    crontab.verify

    if initial
      task_states = crontab.tasks.map { |task| TaskState.new(task: task, schedule: @schedule) }
      task_states.sort_by! { |state| (state.task.group == "$exclusive" ? 0 : 1) }
      return ScheduleLoadResult.new(
        task_states: task_states,
        deferred_tasks: Hash(String, Task).new,
        autosave: crontab.global.autosave,
        print_report: crontab.global.print_report)
    end

    plan = ScheduleReloadPlanner.new(current_task_states, crontab.tasks).plan
    retained = [] of TaskState

    plan.entries.each do |entry|
      if entry.keep_current
        entry.current.not_nil!.keep!
        retained << entry.current.not_nil!
      elsif entry.retire_current
        entry.current.not_nil!.retire!
        retained << entry.current.not_nil!
      else
        next_state = TaskState.new(task: entry.task, schedule: @schedule)
        if entry.preserve_state
          next_state.apply_snapshot(entry.current.not_nil!.state_snapshot)
        end
        retained << next_state
      end
    end

    plan.retiring_removed.each do |task_state|
      task_state.retire!
      retained << task_state unless retained.includes?(task_state)
    end

    retained = retained.uniq
    retained.sort_by! { |state| (state.task.group == "$exclusive" ? 0 : 1) }

    ScheduleLoadResult.new(
      task_states: retained,
      deferred_tasks: plan.deferred_tasks,
      autosave: crontab.global.autosave,
      print_report: crontab.global.print_report)
  end
end
