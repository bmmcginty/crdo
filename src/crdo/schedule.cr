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
  @state_store : ScheduleStateStore
  @reporter : ScheduleReporter
  @pass_planner : SchedulePassPlanner
  @task_lifecycle : ScheduleTaskLifecycle? = nil
  @event_applier : ScheduleEventApplier? = nil

  delegate :select, to: @schedule
  getter immediate, filter, clock, previous_now, current_now, autosave

  def initialize(@test, @immediate, @filter, @crontab, @clock : Clock = SystemClock.new, @loop_waiter : LoopWaiter = SelectLoopWaiter.new)
    @state_store = ScheduleStateStore.new(@crontab)
    @reporter = ScheduleReporter.new(@clock)
    @pass_planner = SchedulePassPlanner.new
  end

  private def task_lifecycle : ScheduleTaskLifecycle
    @task_lifecycle ||= ScheduleTaskLifecycle.new(self, @reporter)
  end

  private def event_applier : ScheduleEventApplier
    @event_applier ||= ScheduleEventApplier.new(self, @reporter)
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

  def all_tasks_have_run_once_since?(start_time)
    do_filter = @filter.size > 0
    ret = true
    @schedule.each do |i|
      if do_filter && !@filter.includes?(i.task.name)
        next
      end
      if !i.has_run_successfully_once_since?(start_time)
        ret = false
      end
    end
    ret
  end

  def clear_dependency_state
    @schedule.each do |parent|
      children = @schedule.select { |i| i.task.parent == parent.task.name }
      children.each do |c|
        c.parent_status[parent.task.name] = false
      end
    end
  end

  def add_tasks(tasks)
    tasks.each do |t|
      @schedule << TaskState.new(task: t, schedule: self)
    end
  end

  def load_task_state?
    @state_store.load_task_state?(self)
  end

  def save_state
    @state_store.save(@schedule)
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
    ct = Crontab.new(@crontab)
    ct.verify
    @autosave = ct.global.autosave
    @print_report = ct.global.print_report
    if initial
      @schedule.clear
      add_tasks(ct.tasks)
      @schedule.sort_by! { |i| (i.task.group == "$exclusive" ? 0 : 1) }
      if !@immediate
        load_task_state?
      end
      clear_dependency_state
      return
    end

    plan = ScheduleReloadPlanner.new(@schedule, ct.tasks).plan
    retained = [] of TaskState
    plan.entries.each do |entry|
      if entry.keep_current
        entry.current.not_nil!.keep!
        retained << entry.current.not_nil!
      elsif entry.retire_current
        entry.current.not_nil!.retire!
        retained << entry.current.not_nil!
      else
        next_state = TaskState.new(task: entry.task, schedule: self)
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

    @schedule = retained.uniq
    @deferred_tasks = plan.deferred_tasks
    @schedule.sort_by! { |i| (i.task.group == "$exclusive" ? 0 : 1) }
    clear_dependency_state
  end

  def autosave(run_state_chan, wait_time = 600.seconds)
    return unless run_state_chan
    while 1
      sleep(wait_time)
      run_state_chan.not_nil!.send(RunState::Save)
    end
  end

  def print_running_report
    @reporter.print_running_report(@schedule)
  end

  def print_report
    @reporter.print_report(@reasons)
  end

  def run_scheduling_pass(controller : ScheduleLoopController, chan : Channel(Time), events : Channel(TaskEvent))
    @current_now = @clock.now
    reasons = [] of TaskWaitState
    @pass_planner.plan(@schedule, @filter).each do |decision|
      task_state = decision.task_state
      reason = decision.wait_state
      case decision.action
      when .start_task?
        spawn do
          task_state.run(chan, events)
        end
        sleep(0.seconds)
        task_lifecycle.started(task_state, chan.receive)
      when .notify_overtime?
        task_lifecycle.notify_overtime(task_state)
      when .none?
      end
      reasons << reason
    end
    controller.update_reasons(reasons)
    @reasons = controller.reasons
    @previous_now = @current_now
  end

  def process_schedule_event(event : ScheduleEvent, controller : ScheduleLoopController) : Bool
    if event.kind.task_completed?
      task_lifecycle.stopped(event.task_event.not_nil!)
    end
    decision = controller.handle_event(
      event,
      @immediate,
      all_tasks_have_run_once_since?(controller.loop_start_time)
    )
    event_applier.apply(decision, controller)
  end

  def loop(run_state_channel : Channel(RunState)? = nil)
    ScheduleLoopRunner.new(self, @clock, @loop_waiter).run(run_state_channel)
  end
end
