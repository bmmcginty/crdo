class ScheduleRuntime
  @schedule : Schedule
  @clock : Clock
  @loop_start_time : Time
  @run_state = RuntimeState::Normal
  @drain_state = DrainState::None
  @shortest_timeout : Time::Span = 1.hour
  @next_autosave_at : Time? = nil

  getter shortest_timeout

  def initialize(@schedule : Schedule, @clock : Clock)
    @loop_start_time = @clock.now
  end

  def run(events : Channel(SchedulerEvent)? = nil)
    event_channel = events || Channel(SchedulerEvent).new
    @schedule.initial_load
    reset_autosave_timer

    loop do
      save_if_autosave_due
      update_drain_state
      if should_exit?
        @schedule.save_state unless @schedule.immediate
        return
      end

      if @run_state.normal? && @drain_state.none?
        run_scheduling_pass(event_channel)
      end

      event = wait_for_event(event_channel, @shortest_timeout)
      break if handle_event(event)
    end
  end

  def run_scheduling_pass(events : Channel(SchedulerEvent))
    pass_time = @clock.now
    reasons = [] of TaskWaitState
    do_filter = @schedule.filter.size > 0
    @schedule.states.each do |task_state|
      next if do_filter && !@schedule.filter.includes?(task_state.task.name)

      reason = task_state.should_run?
      if reason[:reason].none?
        start_task(task_state, events)
      elsif task_state.should_notify_overtime?
        task_state.notify_overtime
      end
      reasons << reason
    end

    reasons.sort_by! { |reason| {reason[:reason], reason[:time], reason[:task].name} }
    @schedule.apply_pass_result(pass_time, reasons)
    @shortest_timeout = next_wake_timeout(reasons)
  end

  def next_wake_timeout(reasons : Array(TaskWaitState)) : Time::Span
    waits = reasons.select { |reason| reason[:reason].wait? }.map { |reason| reason[:time] }
    wait_time = waits.empty? ? 1.hour : waits.min
    if next_autosave = @next_autosave_at
      autosave_wait = next_autosave - @clock.now
      autosave_wait = 0.seconds if autosave_wait < 0.seconds
      return {wait_time, autosave_wait}.min
    end
    wait_time
  end

  private def start_task(task_state : TaskState, events : Channel(SchedulerEvent))
    start_time = @clock.now
    task_state.started(start_time)
    @schedule.reporter.started(task_state, start_time)
    spawn do
      task_state.run(start_time, events)
    end
    sleep(0.seconds)
  end

  private def wait_for_event(events : Channel(SchedulerEvent), wait_time : Time::Span) : SchedulerEvent
    select
    when event = events.receive
      event
    when timeout(wait_time)
      SchedulerEvent.new(kind: SchedulerEventKind::Timeout, task_event: nil)
    end
  end

  private def handle_event(event : SchedulerEvent) : Bool
    case event.kind
    when .task_stopped?
      stopped(event.task_event.not_nil!)
      return @schedule.immediate && all_tasks_have_run_once_since?(@schedule.states, @schedule.filter, @loop_start_time)
    when .reload_requested?
      @schedule.reload_config
      reset_autosave_timer
    when .save_requested?
      return invalid_transition(event.kind) unless @run_state.normal?
      @schedule.save_state
    when .print_report_requested?
      @schedule.print_report
    when .print_running_report_requested?
      @schedule.print_running_report
    when .exit_requested?
      return invalid_transition(event.kind) unless @run_state.normal?
      @run_state = RuntimeState::Exit
      @drain_state = DrainState::Draining
      @schedule.reporter.run_state_changed(@run_state)
    when .timeout?
    end
    false
  end

  private def invalid_transition(requested : SchedulerEventKind) : Bool
    @schedule.reporter.invalid_transition(requested, @run_state, @drain_state)
    false
  end

  private def stopped(event : TaskEvent)
    task_state = event[0]
    task_state.stopped(status: event[1], last_command_index: event[2], stop_time: event[3])
    @schedule.reporter.stopped(task_state, event[1], @schedule.next_task_wait(task_state))
    if task_state.retiring && !task_state.running?
      @schedule.remove_task(task_state)
      @schedule.promote_deferred_replacement(task_state.task.name, task_state.state_snapshot)
    elsif @schedule.has_deferred_replacement?(task_state.task.name)
      @schedule.promote_deferred_replacement(task_state.task.name, task_state.state_snapshot)
    end
  end

  private def reset_autosave_timer
    @next_autosave_at = @schedule.autosave > 0.seconds ? @clock.now + @schedule.autosave : nil
  end

  private def save_if_autosave_due
    return unless next_autosave = @next_autosave_at
    return if @clock.now < next_autosave

    @schedule.save_state
    reset_autosave_timer
  end

  private def update_drain_state
    if @drain_state.draining? && @schedule.running.size == 0
      @drain_state = DrainState::Drained
    end
  end

  private def should_exit?
    @drain_state.drained? && @run_state.exit?
  end

  private def all_tasks_have_run_once_since?(task_states : Array(TaskState), filter : Set(String), start_time : Time) : Bool
    do_filter = filter.size > 0
    task_states.each do |task_state|
      next if do_filter && !filter.includes?(task_state.task.name)

      return false unless task_state.has_run_successfully_once_since?(start_time)
    end
    true
  end
end
