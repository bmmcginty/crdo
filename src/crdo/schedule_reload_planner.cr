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
