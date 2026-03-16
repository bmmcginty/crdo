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

  delegate :select, to: @schedule
  getter immediate, filter, clock, previous_now, current_now

  def initialize(@test, @immediate, @filter, @crontab, @clock : Clock = SystemClock.new, @loop_waiter : LoopWaiter = SelectLoopWaiter.new)
    @state_store = ScheduleStateStore.new(@crontab)
    @reporter = ScheduleReporter.new(@clock)
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

  private def apply_scheduling_pass(controller : ScheduleLoopController, do_filter : Bool, chan : Channel(Time), events : Channel(TaskEvent))
    @current_now = @clock.now
    reasons = [] of TaskWaitState
    @schedule.each do |i|
      if do_filter && !@filter.includes?(i.task.name)
        next
      end
      reason = i.should_run?
      if reason[:reason].none?
        spawn do
          i.run(chan, events)
        end
        sleep(0.seconds)
        started(i, chan.receive)
      else
        if i.should_notify_overtime?
          notify_overtime(i)
        end
      end
      reasons << reason
    end
    controller.update_reasons(reasons)
    @reasons = controller.reasons
    @previous_now = @current_now
  end

  private def handle_schedule_event(event : ScheduleEvent, controller : ScheduleLoopController) : Bool
    case event.kind
    when .run_state_request?
      requested = event.run_state.not_nil!
      case controller.handle_run_state_request(requested)
      when .print_report?
        print_report
      when .print_running_report?
        print_running_report
      when .reload?
        load
      when .invalid?
        @reporter.invalid_transition(requested, controller.run_state, controller.drain_state)
      when .save?
        save_state
      when .transition?
        @reporter.run_state_changed(controller.run_state)
      end
      false
    when .task_completed?
      stopped(event.task_event.not_nil!)
      controller.immediate_complete?(@immediate, all_tasks_have_run_once_since?(controller.loop_start_time))
    when .timeout?
      false
    else
      false
    end
  end

  def loop(run_state_channel : Channel(RunState)? = nil)
    chan = Channel(Time).new
    events = Channel(Tuple(TaskState, Int32, Int32, Time)).new
    controller = ScheduleLoopController.new(@clock)
    do_filter = @filter.size > 0
    load(true)
    if @autosave > 0.seconds
      spawn do
        autosave(run_state_channel, @autosave)
      end
      sleep(0.seconds)
    end
    while 1
      case controller.next_action(running.size, @immediate)
      when .save_and_exit?
        save_state
        exit
      when .exit?
        exit
      when .schedule_pass?
        apply_scheduling_pass(controller, do_filter, chan, events)
      when .wait?
      end
      event = @loop_waiter.wait(run_state_channel, events, controller.shortest_timeout)
      if handle_schedule_event(event, controller)
        break
      end
    end
  end

  def notify_overtime(task)
    task.notify_overtime
  end

  def started(task, start_time)
    task.started(start_time)
    @reporter.started(task, start_time)
  end

  def stopped(x)
    task_state = x[0]
    task_state.stopped(status: x[1], last_command_index: x[2], stop_time: x[3])
    @reporter.stopped(task_state, x[1], next_task_wait(task_state))
    if task_state.retiring && !task_state.running?
      @schedule.delete(task_state)
      activate_deferred_task(task_state.task.name, task_state.state_snapshot)
    elsif @deferred_tasks.has_key?(task_state.task.name)
      activate_deferred_task(task_state.task.name, task_state.state_snapshot)
    end
  end
end
