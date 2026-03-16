class TaskState
  @errors = [] of Exception
  @schedule : Schedule
  @task : Task
  @process_runner : TaskProcessRunner
  @mailer : TaskMailer
  @run_evaluator : TaskRunEligibilityEvaluator
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

  def initialize(@task, @schedule, @process_runner = TaskProcessRunner.new, @mailer = TaskMailer.new, @run_evaluator = TaskRunEligibilityEvaluator.new)
  end

  def run_time
    Time.local - @current_start.not_nil!
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

  def should_notify_overtime?
    if !@running
      return false
    end
    if @overtime_occured
      return false
    end
    if @task.global.ignore_overtime
      return false
    end
    if @task.every && (Time.local - @current_start.not_nil!) > @task.every.not_nil!
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

  def next_scheduled_time(now = Time.local)
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

  def stopped(status : Int32, last_command_index : Int32, stop_time : Time)
    @running = false
    @last_start = @current_start
    @last_status = status
    @last_stop = stop_time
    success = success?
    if @task.global.test && @task.global.error
      success = false
    end
    @parent_status.keys.each do |k|
      @parent_status[k] = false
    end
    if success
      children = @schedule.select { |i| i.task.parent == @task.name }
      children.each do |c|
        c.parent_status[@task.name] = true
      end
    else
      if @task.error_command
        spawn do
          ec = @task.hydrate_command(@task.error_command.not_nil!)
          Process.run(command: ec[0], args: ec[1..-1], chdir: @task.global.workdir)
        end
        sleep 0.seconds
      end
      if @task.global.mail
        @mailer.notify_failure(@task, @last_status, log_dn(@last_start.as(Time)))
      end
    end
  end

  def run(start_channel, events_channel)
    @errors.clear
    ts = Time.local
    start_channel.send(ts)
    last_command = -1
    rc = 0
    @task.commands.each_with_index do |c, idx|
      last_command += 1
      t = @task.hydrate_command(c)
      begin
        rc = run(args: t, idx: idx, start_time: ts)
      rescue exc
        rc = 999
        @errors << exc
      end
      break if rc != 0
    end
    events_channel.send({self, rc, last_command, Time.local})
  end

  def run(args : Array(String), idx : Int32, start_time : Time)
    begin
      ret = @process_runner.run(@task, args, idx, start_time, @schedule.test)
    rescue e
      @errors << e
      raise e
    end
    ret
  end

  def should_run? : TaskWaitState
    @run_evaluator.evaluate(
      self,
      TaskRunContext.new(
        running: @schedule.running,
        immediate: @schedule.immediate,
        filter: @schedule.filter,
        now: Time.local))
  end
end
