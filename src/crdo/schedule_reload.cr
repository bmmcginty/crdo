class ScheduleReloadPlanner
  @existing : Array(TaskState)
  @incoming : Array(Task)

  def initialize(@existing, @incoming)
  end

  def plan
    existing_by_name = Hash(String, TaskState).new
    @existing.each do |task_state|
      existing_by_name[task_state.task.name] = task_state unless task_state.retiring
    end

    entries = [] of ScheduleReloadPlanEntry
    deferred_tasks = Hash(String, Task).new
    incoming_names = Set(String).new

    @incoming.each do |task|
      incoming_names << task.name
      current = existing_by_name[task.name]?
      if current && current.task.signature == task.signature
        entries << ScheduleReloadPlanEntry.new(
          task: task,
          current: current,
          keep_current: true,
          preserve_state: false,
          retire_current: false)
        existing_by_name.delete(task.name)
      elsif current && current.running?
        deferred_tasks[task.name] = task
        entries << ScheduleReloadPlanEntry.new(
          task: task,
          current: current,
          keep_current: false,
          preserve_state: false,
          retire_current: true)
        existing_by_name.delete(task.name)
      else
        entries << ScheduleReloadPlanEntry.new(
          task: task,
          current: current,
          keep_current: false,
          preserve_state: !current.nil?,
          retire_current: false)
        existing_by_name.delete(task.name) if current
      end
    end

    retiring_removed = @existing.select do |task_state|
      task_state.running? && !incoming_names.includes?(task_state.task.name)
    end

    ScheduleReloadPlan.new(
      entries: entries,
      retiring_removed: retiring_removed,
      deferred_tasks: deferred_tasks)
  end
end

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

class ScheduleDependencyResetter
  def reset(task_states : Array(TaskState))
    task_states.each do |parent|
      children = task_states.select { |state| state.task.parent == parent.task.name }
      children.each do |child|
        child.parent_status[parent.task.name] = false
      end
    end
  end
end
