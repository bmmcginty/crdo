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
    state_data.each do |ts|
      task_state = schedule[ts["name"].as_s]?
      if !task_state
        err = true
        next
      end
      task_state.not_nil!.set_state(ts)
    end
    err == false
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

class ScheduleReporter
  def next_task_wait(state : TaskState)
    wait_state = state.should_run?
    if wait_state[:reason].wait?
      next_time = Time.local + wait_state[:time]
      "#{format_time_span(wait_state[:time])} (#{next_time})"
    else
      next_time = state.next_scheduled_time
      "#{format_time_span(next_time - Time.local)} (#{next_time})"
    end
  end

  def print_running_report(schedule : Array(TaskState))
    running = schedule.select(&.running?)
    running.sort_by! { |i| i.task.name }
    running.each do |i|
      puts "#{i.task.name}, #{i.run_time}"
    end
    puts "-----"
  end

  def print_report(reasons : Array(TaskWaitState))
    puts "as of #{Time.local}"
    reasons.each do |r|
      puts "#{r[:task].name}, #{r[:reason].none? || r[:reason].already_running? ? "running" : r[:reason].to_s}: #{r[:text]} #{format_time_span(r[:time])}"
    end
    puts "-----"
  end

  def started(task : TaskState, start_time : Time)
    puts "start #{task.task.name} at #{start_time}"
  end

  def stopped(task : TaskState, status : Int32, next_wait : String)
    duration = if task.last_start && task.last_stop
                 task.last_stop.not_nil! - task.last_start.not_nil!
               else
                 0.seconds
               end
    puts "stop #{task.task.name} rc=#{status} duration=#{format_time_span(duration)} next=#{next_wait}"
  end

  def run_state_changed(run_state : RunState)
    puts "run state #{run_state}"
  end

  def invalid_transition(requested : RunState, current : RunState, drain_state : DrainState)
    puts "requested run state #{requested} but currently have run state #{current} drain state #{drain_state}"
  end
end
