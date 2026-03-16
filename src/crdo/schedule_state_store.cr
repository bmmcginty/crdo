class ScheduleStateStore
  @crontab : String

  def initialize(@crontab)
  end

  def load_state_data
    path = @crontab + ".state"
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

  def load_task_state?(schedule : Schedule)
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

  def snapshot_for(data : JSON::Any)
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

  def save(schedule : Array(TaskState))
    path = @crontab + ".state"
    dest = Path[path].expand(home: true).to_s
    File.write(
      dest + ".tmp",
      {
        version: 2,
        tasks: schedule,
      }.to_json)
    File.rename(
      dest + ".tmp",
      dest)
  end
end
