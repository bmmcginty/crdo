class ScheduleLoopRunner
  @schedule : Schedule
  @clock : Clock
  @loop_waiter : LoopWaiter
  @pass_runner : SchedulePassRunner
  @task_lifecycle : ScheduleTaskLifecycle
  @completion_check : ScheduleCompletionCheck
  @loop_start_time : Time
  @run_state = RunState::Normal
  @drain_state = DrainState::None
  @shortest_timeout : Time::Span = 1.hour

  def initialize(@schedule : Schedule, @clock : Clock, @loop_waiter : LoopWaiter)
    @task_lifecycle = ScheduleTaskLifecycle.new(@schedule, @schedule.reporter)
    @pass_runner = SchedulePassRunner.new(SchedulePassPlanner.new, @task_lifecycle, @clock)
    @completion_check = ScheduleCompletionCheck.new
    @loop_start_time = @clock.now
  end

  def run(run_state_channel : Channel(RunState)? = nil)
    chan = Channel(Time).new
    events = Channel(TaskEvent).new
    @schedule.load(true)
    if @schedule.autosave > 0.seconds
      spawn do
        autosave(run_state_channel, @schedule.autosave)
      end
      sleep(0.seconds)
    end

    while 1
      if @drain_state.draining? && @schedule.running.size == 0
        @drain_state = DrainState::Drained
      end

      if @drain_state.drained? && @run_state.exit?
        unless @schedule.immediate
          @schedule.save_state
        end
        exit
      end

      if @run_state.normal? && @drain_state.none?
        run_scheduling_pass(chan, events)
      end

      event = @loop_waiter.wait(run_state_channel, events, @shortest_timeout)
      break if process_event(event)
    end
  end

  private def run_scheduling_pass(chan : Channel(Time), events : Channel(TaskEvent))
    result = @pass_runner.run(@schedule.task_states, @schedule.filter, chan, events)
    reasons = result.reasons
    wait_timeouts = reasons.select { |reason| reason[:reason].wait? }.map { |reason| reason[:time] }
    @shortest_timeout = wait_timeouts.empty? ? 1.hour : wait_timeouts.min
    reasons.sort_by! { |reason| {reason[:reason], reason[:time], reason[:task].name} }
    @schedule.apply_pass_result(result, reasons)
  end

  private def process_event(event : ScheduleEvent) : Bool
    case event.kind
    when .task_completed?
      @task_lifecycle.stopped(event.task_event.not_nil!)
      if @schedule.immediate && @completion_check.all_tasks_have_run_once_since?(@schedule.task_states, @schedule.filter, @loop_start_time)
        return true
      end
      false
    when .run_state_request?
      handle_run_state_request(event.run_state.not_nil!)
      false
    when .timeout?
      false
    else
      false
    end
  end

  private def handle_run_state_request(requested : RunState)
    if requested.print_report?
      @schedule.print_report
      return
    end
    if requested.print_running_report?
      @schedule.print_running_report
      return
    end
    if requested.reload?
      @schedule.load
      return
    end
    if !@run_state.normal?
      @schedule.reporter.invalid_transition(requested, @run_state, @drain_state)
      return
    end
    if requested.save?
      @schedule.save_state
      return
    end
    @run_state = requested
    @drain_state = DrainState::Draining
    @schedule.reporter.run_state_changed(@run_state)
  end

  private def autosave(run_state_channel : Channel(RunState)?, wait_time : Time::Span)
    return unless run_state_channel
    while 1
      sleep(wait_time)
      run_state_channel.not_nil!.send(RunState::Save)
    end
  end
end
