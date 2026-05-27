class Schedule
  @schedule = [] of TaskState
  @test : Bool
  @immediate : Bool
  @filter : Set(String)
  @crontab : String
  @clock : Clock
  @loop_waiter : LoopWaiter
  @autosave : Time::Span = 0.seconds
  property :test
  @print_report = true
  @reasons = [] of TaskWaitState
  @deferred_tasks = Hash(String, Task).new
  @previous_now : Time? = nil
  @current_now : Time? = nil
  @reloader : ScheduleReloader? = nil
  @dependency_state : ScheduleDependencyState? = nil
  @completion_check : ScheduleCompletionCheck? = nil
  @reporter : ScheduleReporter
  @pass_planner : SchedulePassPlanner
  @pass_runner : SchedulePassRunner? = nil
  @task_lifecycle : ScheduleTaskLifecycle? = nil
  @event_actions : ScheduleEventActions? = nil

  delegate :select, to: @schedule
  getter immediate, filter, clock, previous_now, current_now, autosave

  def initialize(@test, @immediate, @filter, @crontab, @clock : Clock = SystemClock.new, @loop_waiter : LoopWaiter = SelectLoopWaiter.new)
    @reporter = ScheduleReporter.new(@clock)
    @pass_planner = SchedulePassPlanner.new
  end

  private def task_lifecycle : ScheduleTaskLifecycle
    @task_lifecycle ||= ScheduleTaskLifecycle.new(self, @reporter)
  end

  private def pass_runner : SchedulePassRunner
    @pass_runner ||= SchedulePassRunner.new(@pass_planner, task_lifecycle, @clock)
  end

  private def event_actions : ScheduleEventActions
    @event_actions ||= ScheduleEventActions.new(self, @reporter)
  end

  private def reloader : ScheduleReloader
    @reloader ||= ScheduleReloader.new(self, @crontab)
  end

  private def dependency_state : ScheduleDependencyState
    @dependency_state ||= ScheduleDependencyState.new
  end

  private def completion_check : ScheduleCompletionCheck
    @completion_check ||= ScheduleCompletionCheck.new
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

  def add_tasks(tasks)
    tasks.each do |t|
      @schedule << TaskState.new(task: t, schedule: self)
    end
  end

  def load_task_state?
    reloader.load_task_state?(self)
  end

  def save_state
    reloader.save(@schedule)
  end

  def activate_deferred_task(name, snapshot)
    next_task = @deferred_tasks.delete(name)
    return unless next_task
    next_state = TaskState.new(task: next_task, schedule: self)
    next_state.apply_snapshot(snapshot)
    @schedule << next_state
  end

  def remove_task(task_state : TaskState)
    @schedule.delete(task_state)
  end

  def deferred_task?(name : String)
    @deferred_tasks.has_key?(name)
  end

  def next_task_wait(state : TaskState)
    @reporter.next_task_wait(state)
  end

  def load(initial = false)
    result = reloader.load(initial, @schedule)
    @autosave = result.autosave
    @print_report = result.print_report
    @schedule = result.task_states
    @deferred_tasks = result.deferred_tasks
    if initial && !@immediate
      load_task_state?
    end
    dependency_state.reset(@schedule)
  end

  def print_running_report
    @reporter.print_running_report(@schedule)
  end

  def print_report
    @reporter.print_report(@reasons)
  end

  def run_scheduling_pass(controller : ScheduleLoopController, chan : Channel(Time), events : Channel(TaskEvent))
    result = pass_runner.run(@schedule, @filter, chan, events)
    controller.update_reasons(result.reasons)
    @reasons = controller.reasons
    @current_now = result.pass_time
    @previous_now = result.pass_time
  end

  def process_schedule_event(event : ScheduleEvent, controller : ScheduleLoopController) : Bool
    if event.kind.task_completed?
      task_lifecycle.stopped(event.task_event.not_nil!)
    end
    control = controller.handle_event(
      event,
      @immediate,
      completion_check.all_tasks_have_run_once_since?(@schedule, @filter, controller.loop_start_time)
    )
    event_actions.apply(control, controller)
  end

  def loop(run_state_channel : Channel(RunState)? = nil)
    ScheduleLoopRunner.new(self, @clock, @loop_waiter).run(run_state_channel)
  end
end
