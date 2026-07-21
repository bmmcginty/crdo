record ScheduleReloadPlanEntry,
  task : Task,
  current : TaskState?,
  keep_current : Bool,
  preserve_state : Bool,
  retire_current : Bool

record ScheduleReloadPlan,
  entries : Array(ScheduleReloadPlanEntry),
  retiring_removed : Array(TaskState),
  deferred_tasks : Hash(String, Task)

record ScheduleLoadResult,
  states : Array(TaskState),
  deferred_tasks : Hash(String, Task),
  autosave : Time::Span,
  print_report : Bool

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

class ScheduleStore
  @crontab_path : String

  def initialize(@crontab_path : String)
  end

  def restore_state(schedule : ScheduleState) : Bool
    err = false
    state_data = load_state_data
    return false unless state_data
    pending = [] of Tuple(TaskState, TaskStateSnapshot)
    state_data.each do |ts|
      task_state = schedule[ts["name"].as_s]?
      if !task_state
        err = true
        next
      end
      pending << {task_state.not_nil!, snapshot_for(ts)}
    end
    return false if err
    pending.each do |task_state, snapshot|
      task_state.apply_snapshot(snapshot)
    end
    true
  end

  def save(task_states : Array(TaskState))
    path = @crontab_path + ".state"
    dest = Path[path].expand(home: true).to_s
    File.write(
      dest + ".tmp",
      {
        version: 2,
        tasks:   task_states,
      }.to_json)
    File.rename(
      dest + ".tmp",
      dest)
  end

  def initial_load : ScheduleLoadResult
    crontab = Crontab.new(@crontab_path)
    crontab.verify

    task_states = crontab.tasks.map { |task| TaskState.new(task: task) }
    task_states.sort_by! { |state| (state.task.group == "$exclusive" ? 0 : 1) }
    ScheduleLoadResult.new(
      states: task_states,
      deferred_tasks: Hash(String, Task).new,
      autosave: crontab.global.autosave,
      print_report: crontab.global.print_report)
  end

  def reload(current_task_states : Array(TaskState)) : ScheduleLoadResult
    crontab = Crontab.new(@crontab_path)
    crontab.verify

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
        next_state = TaskState.new(task: entry.task)
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
      states: retained,
      deferred_tasks: plan.deferred_tasks,
      autosave: crontab.global.autosave,
      print_report: crontab.global.print_report)
  end

  def reset_dependencies(task_states : Array(TaskState))
    task_states.each do |parent|
      children = task_states.select { |state| state.task.parent == parent.task.name }
      children.each do |child|
        child.parent_status[parent.task.name] = false
      end
    end
  end

  private def load_state_data
    path = @crontab_path + ".state"
    src = Path[path].expand(home: true)
    return nil unless File.exists?(src)
    state = JSON.parse(File.read(src))
    if state.raw.is_a?(Array)
      state.as_a
    else
      version = state["version"]?.try(&.as_i?) || 1
      raise Exception.new("unsupported state version #{version}") unless version == 2
      state["tasks"].as_a
    end
  end

  private def snapshot_for(data : JSON::Any)
    TaskStateSnapshot.new(
      last_start: if t = data["last_start"]?.try(&.as_i64?)
        Time.unix(t).to_local
      elsif t = data["last_start_ms"]?.try(&.as_i64?)
        Time.unix_ms(t).to_local
      else
        nil
      end,
      last_stop: if t = data["last_stop"]?.try(&.as_i64?)
        Time.unix(t).to_local
      elsif t = data["last_stop_ms"]?.try(&.as_i64?)
        Time.unix_ms(t).to_local
      else
        nil
      end,
      last_status: data["last_status"]?.try(&.as_i?) || -1)
  end
end
