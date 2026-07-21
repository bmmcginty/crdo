class Schedule
  @schedule = [] of TaskState
  @test : Bool
  @immediate : Bool
  @filter : Set(String)
  @crontab : String
  @clock : Clock
  @autosave : Time::Span = 0.seconds
  property :test
  @print_report = true
  @reasons = [] of TaskWaitState
  @deferred_tasks = Hash(String, Task).new
  @previous_now : Time? = nil
  @config_state : ScheduleConfigState? = nil
  @reporter : ScheduleReporter
  @mail_failures = [] of MailFailure

  delegate :select, to: @schedule
  getter immediate, filter, clock, previous_now, autosave, reporter, mail_failures

  def initialize(@test, @immediate, @filter, @crontab, @clock : Clock = SystemClock.new)
    @reporter = ScheduleReporter.new(@clock)
  end

  private def config_state : ScheduleConfigState
    @config_state ||= ScheduleConfigState.new(self, @crontab)
  end

  def [](name : String)
    @schedule.find! { |i| i.task.name == name }
  end

  def []?(name : String)
    @schedule.find { |i| i.task.name == name }
  end

  def running
    @schedule.select(&.running?)
  end

  def states
    @schedule
  end

  def add_tasks(tasks)
    tasks.each do |t|
      @schedule << TaskState.new(task: t, schedule: self)
    end
  end

  def load_task_state?
    config_state.load_task_state?(self)
  end

  def save_state
    config_state.save(@schedule)
  end

  def promote_deferred_replacement(name, snapshot)
    next_task = @deferred_tasks.delete(name)
    return unless next_task
    next_state = TaskState.new(task: next_task, schedule: self)
    next_state.apply_snapshot(snapshot)
    @schedule << next_state
  end

  def remove_task(task_state : TaskState)
    @schedule.delete(task_state)
  end

  def has_deferred_replacement?(name : String)
    @deferred_tasks.has_key?(name)
  end

  def task_wait_state(state : TaskState, now : Time = @clock.now)
    TaskRunEligibilityEvaluator.new.evaluate(
      state,
      TaskRunContext.new(
        running: running,
        immediate: @immediate,
        filter: @filter,
        previous_now: @previous_now,
        now: now))
  end

  def next_task_wait(state : TaskState)
    @reporter.next_task_wait(state, task_wait_state(state))
  end

  def initial_load
    result = config_state.load(true, @schedule)
    @autosave = result.autosave
    @print_report = result.print_report
    @schedule = result.states
    @deferred_tasks = result.deferred_tasks
    if !@immediate
      load_task_state?
    end
    config_state.reset_dependencies(@schedule)
  end

  def reload_config
    result = config_state.load(false, @schedule)
    @autosave = result.autosave
    @print_report = result.print_report
    @schedule = result.states
    @deferred_tasks = result.deferred_tasks
    config_state.reset_dependencies(@schedule)
  end

  def print_running_report
    @reporter.print_running_report(@schedule)
  end

  def print_report
    @reporter.print_report(@reasons, @mail_failures)
  end

  def apply_pass_result(pass_time : Time, reasons : Array(TaskWaitState))
    @reasons = reasons
    @previous_now = pass_time
  end

  def record_mail_failure(task_name : String, log_dir : String, message : String)
    @mail_failures << MailFailure.new(
      task_name: task_name,
      log_dir: log_dir,
      message: message,
      time: @clock.now)
  end

  def loop(events : Channel(SchedulerEvent)? = nil)
    ScheduleRuntime.new(self, @clock).run(events)
  end
end
