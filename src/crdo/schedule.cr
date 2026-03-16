class Schedule
  @schedule = [] of TaskState
  @test : Bool
  @immediate : Bool
  @filter : Set(String)
  @crontab : String
  @autosave : Time::Span = 0.seconds
  property :test
  @print_report = true
  @reasons = [] of TaskWaitState
  @deferred_tasks = Hash(String, Task).new
  @state_store : ScheduleStateStore
  @reporter : ScheduleReporter

  delegate :select, to: @schedule
  getter immediate, filter

  def initialize(@test, @immediate, @filter, @crontab)
    @state_store = ScheduleStateStore.new(@crontab)
    @reporter = ScheduleReporter.new
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

    existing = Hash(String, TaskState).new
    @schedule.each do |task_state|
      existing[task_state.task.name] = task_state unless task_state.retiring
    end

    retained = [] of TaskState
    deferred = Hash(String, Task).new
    incoming_names = Set(String).new

    ct.tasks.each do |task|
      incoming_names << task.name
      current = existing[task.name]?
      if current && current.task.signature == task.signature
        current.keep!
        retained << current
        existing.delete(task.name)
      elsif current && current.running?
        current.retire!
        retained << current
        deferred[task.name] = task
        existing.delete(task.name)
      else
        next_state = TaskState.new(task: task, schedule: self)
        if current
          next_state.apply_snapshot(current.state_snapshot)
          existing.delete(task.name)
        end
        retained << next_state
      end
    end

    @schedule.each do |task_state|
      next unless task_state.running?
      next if incoming_names.includes?(task_state.task.name)
      task_state.retire!
      retained << task_state unless retained.includes?(task_state)
    end

    @schedule = retained.uniq
    @deferred_tasks = deferred
    @schedule.sort_by! { |i| (i.task.group == "$exclusive" ? 0 : 1) }
    clear_dependency_state
  end

  def autosave(run_state_chan, wait_time = 600.seconds)
    while 1
      sleep(wait_time)
      run_state_chan.send(RunState::Save)
    end
  end

  def print_running_report
    @reporter.print_running_report(@schedule)
  end

  def print_report
    @reporter.print_report(@reasons)
  end

  def loop(run_state_channel : Channel(RunState)? = nil)
    loop_start_time = Time.local
    reasons = [] of TaskWaitState
    chan = Channel(Time).new
    events = Channel(Tuple(TaskState, Int32, Int32, Time)).new
    drain_state = DrainState::None
    run_state = RunState::Normal
    shortest_timeout = 1.hour
    do_filter = @filter.size > 0
    load(true)
    if @autosave > 0.seconds
      spawn do
        autosave(run_state_channel, @autosave)
      end
      sleep(0.seconds)
    end
    while 1
      if drain_state.draining? && @schedule.none? { |i| i.running? }
        drain_state = DrainState::Drained
      end
      if drain_state.drained?
        if run_state.exit?
          if !@immediate
            save_state
          end
        end
        if run_state.exit?
          exit
        end
      end
      if run_state.normal? && drain_state.none?
        reasons.clear
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
        timeouts = reasons.select { |i| i[:reason].wait? }.map { |i| i[:time] }
        shortest_timeout = timeouts.size > 0 ? timeouts.min : 1.hour
        reasons.sort_by! do |i|
          ({i[:reason], i[:time], i[:task].name})
        end
        @reasons = reasons
      end
      select
      when t = run_state_channel.receive
        if t.print_report?
          print_report
          next
        end
        if t.print_running_report?
          print_running_report
          next
        end
        if t.reload?
          load
          next
        end
        if !run_state.normal?
          @reporter.invalid_transition(t, run_state, drain_state)
          next
        end
        if t.save?
          save_state
          next
        end
        run_state = t
        drain_state = DrainState::Draining
        @reporter.run_state_changed(run_state)
        next
      when x = events.receive
        stopped(x)
        if @immediate && all_tasks_have_run_once_since?(loop_start_time)
          break
        end
        next
      when timeout(shortest_timeout)
        next
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
