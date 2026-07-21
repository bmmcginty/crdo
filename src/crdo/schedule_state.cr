class ScheduleState
  @states = [] of TaskState
  @test : Bool
  @immediate : Bool
  @filter : Set(String)
  @crontab : String
  @clock : Clock
  @autosave : Time::Span = 0.seconds
  property :test
  @print_report = true
  @reasons = [] of TaskRunDecision
  @deferred_tasks = Hash(String, Task).new
  @previous_now : Time? = nil
  @store : ScheduleStore? = nil
  @reporter : ScheduleReporter
  @mail_failures = [] of MailFailure

  delegate :select, to: @states
  getter immediate, filter, clock, previous_now, autosave, reporter, mail_failures

  def initialize(@test, @immediate, @filter, @crontab, @clock : Clock = SystemClock.new, output : IO = STDOUT)
    @reporter = ScheduleReporter.new(@clock, output)
  end

  private def store : ScheduleStore
    @store ||= ScheduleStore.new(@crontab)
  end

  def [](name : String)
    @states.find! { |i| i.task.name == name }
  end

  def []?(name : String)
    @states.find { |i| i.task.name == name }
  end

  def running
    @states.select(&.running?)
  end

  def states
    @states
  end

  def add_tasks(tasks)
    tasks.each do |t|
      @states << TaskState.new(task: t)
    end
  end

  def restore_state
    store.restore_state(self)
  end

  def save_state
    store.save(@states)
  end

  def promote_deferred_replacement(name, snapshot)
    next_task = @deferred_tasks.delete(name)
    return unless next_task
    next_state = TaskState.new(task: next_task)
    next_state.apply_snapshot(snapshot)
    @states << next_state
  end

  def remove_task(task_state : TaskState)
    @states.delete(task_state)
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

  def load_initial_state
    result = store.initial_load
    @autosave = result.autosave
    @print_report = result.print_report
    @states = result.states
    @deferred_tasks = result.deferred_tasks
    if !@immediate
      restore_state
    end
    store.reset_dependencies(@states)
  end

  def reload_config
    result = store.reload(@states)
    @autosave = result.autosave
    @print_report = result.print_report
    @states = result.states
    @deferred_tasks = result.deferred_tasks
    store.reset_dependencies(@states)
  end

  def print_running_report
    @reporter.print_running_report(@states)
  end

  def print_report
    @reporter.print_report(@reasons, @mail_failures)
  end

  def apply_pass_result(pass_time : Time, reasons : Array(TaskRunDecision))
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
end
