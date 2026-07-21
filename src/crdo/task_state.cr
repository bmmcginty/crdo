class TaskState
  @errors = [] of Exception
  @task : Task
  @process_runner : TaskProcessRunner
  @mailer : TaskMailer
  @current_start : Time? = nil
  @last_start : Time? = nil
  @last_stop : Time? = nil
  @last_status = -1
  @running = false
  @overtime_occured = false
  @parent_status = Hash(String, Bool).new
  @sp : Process? = nil
  @retiring = false
  getter parent_status, task, retiring, last_start, last_stop, last_status

  def initialize(@task, @process_runner = TaskProcessRunner.new, @mailer = TaskMailer.new)
  end

  def run_time(now : Time)
    now - @current_start.not_nil!
  end

  def task=(task : Task)
    @task = task
  end

  def retire!
    @retiring = true
  end

  def keep!
    @retiring = false
  end

  def state_snapshot
    TaskStateSnapshot.new(
      last_start: @last_start,
      last_stop: @last_stop,
      last_status: @last_status)
  end

  def apply_snapshot(snapshot : TaskStateSnapshot)
    @last_start = snapshot.last_start
    @last_stop = snapshot.last_stop
    @last_status = snapshot.last_status
    @parent_status.clear
    @overtime_occured = false
  end

  def should_notify_overtime?(now : Time)
    if !@running
      return false
    end
    if @overtime_occured
      return false
    end
    if @task.global.ignore_overtime
      return false
    end
    if @task.every && (now - @current_start.not_nil!) > @task.every.not_nil!
      return true
    end
    false
  end

  def notify_overtime
    @overtime_occured = true
    @mailer.notify_overtime(@task)
  end

  def to_json(json : JSON::Builder)
    json.object do
      json.field "name", @task.name
      json.field "last_status", @last_status
      json.field "last_stop_ms", (@last_stop ? @last_stop.not_nil!.to_utc.to_unix_ms : nil)
      json.field "last_start_ms", (@last_start ? @last_start.not_nil!.to_utc.to_unix_ms : nil)
    end
  end

  def set_state(data : JSON::Any)
    @last_start = if t = data["last_start"]?.try(&.as_i64?)
                    Time.unix(t).to_local
                  elsif t = data["last_start_ms"]?.try(&.as_i64?)
                    Time.unix_ms(t).to_local
                  else
                    nil
                  end
    @last_stop = if t = data["last_stop"]?.try(&.as_i64?)
                   Time.unix(t).to_local
                 elsif t = data["last_stop_ms"]?.try(&.as_i64?)
                   Time.unix_ms(t).to_local
                 else
                   nil
                 end
    @last_status = data["last_status"]?.try(&.as_i?) || -1
    @parent_status.clear
    @overtime_occured = false
  end

  def has_run_successfully_once_since?(ts : Time)
    success? && @last_start && @last_stop && @last_stop.not_nil! >= @last_start.not_nil! && @last_start.not_nil! >= ts
  end

  def running?
    @running
  end

  def started(start_time : Time)
    @overtime_occured = false
    @running = true
    @current_start = start_time
  end

  def success?
    @last_status == 0
  end

  def log_dn(ts)
    @process_runner.log_dn(@task, ts)
  end

  def next_scheduled_time(now : Time)
    if @task.when_specs.size > 0
      return @task.when_specs.map { |matcher| matcher.find_next(now) }.min
    end
    return now unless @task.every
    base = if @task.use_stop_time
             @last_stop || @last_start
           else
             @last_start
           end
    return now unless base
    base.not_nil! + @task.every.not_nil!
  end

  def mark_stopped(status : Int32, stop_time : Time)
    @running = false
    @last_start = @current_start
    @last_status = status
    @last_stop = stop_time
  end

  def run(start_time : Time, events : Channel(SchedulerEvent), test : Bool, clock : Clock)
    @errors.clear
    last_command = -1
    rc = 0
    @task.commands.each_with_index do |c, idx|
      last_command += 1
      t = @task.hydrate_command(c)
      begin
        rc = run(args: t, idx: idx, start_time: start_time, test: test)
      rescue exc
        rc = 999
        @errors << exc
      end
      break if rc != 0
    end
    events.send(SchedulerEvent.new(
      kind: SchedulerEventKind::TaskStopped,
      task_stopped: TaskStoppedEvent.new(
        task_state: self,
        status: rc,
        last_command_index: last_command,
        stop_time: clock.now)))
  end

  def run(args : Array(String), idx : Int32, start_time : Time, test : Bool)
    begin
      ret = @process_runner.run(@task, args, idx, start_time, test)
    rescue e
      @errors << e
      raise e
    end
    ret
  end
end
