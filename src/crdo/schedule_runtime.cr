class ScheduleLoopController
  @clock : Clock
  @loop_start_time : Time
  @drain_state = DrainState::None
  @run_state = RunState::Normal
  @shortest_timeout : Time::Span = 1.hour
  @reasons = [] of TaskWaitState

  getter loop_start_time, shortest_timeout, reasons, run_state, drain_state

  def initialize(@clock : Clock)
    @loop_start_time = @clock.now
  end

  def next_action(running_count : Int32, immediate : Bool) : ScheduleLoopAction
    if @drain_state.draining? && running_count == 0
      @drain_state = DrainState::Drained
    end
    if @drain_state.drained? && @run_state.exit?
      return immediate ? ScheduleLoopAction::Exit : ScheduleLoopAction::SaveAndExit
    end
    if @run_state.normal? && @drain_state.none?
      return ScheduleLoopAction::SchedulePass
    end
    ScheduleLoopAction::Wait
  end

  def update_reasons(reasons : Array(TaskWaitState))
    timeouts = reasons.select { |i| i[:reason].wait? }.map { |i| i[:time] }
    @shortest_timeout = timeouts.size > 0 ? timeouts.min : 1.hour
    reasons.sort_by! do |i|
      ({i[:reason], i[:time], i[:task].name})
    end
    @reasons = reasons
  end

  def handle_event(event : ScheduleEvent, immediate : Bool, all_tasks_have_run_once : Bool = false) : ScheduleEventDecision
    case event.kind
    when .run_state_request?
      handle_run_state_request(event.run_state.not_nil!)
    when .task_completed?
      if immediate && all_tasks_have_run_once
        ScheduleEventDecision.new(action: ScheduleEventAction::BreakLoop, requested_run_state: nil)
      else
        ScheduleEventDecision.new(action: ScheduleEventAction::None, requested_run_state: nil)
      end
    when .timeout?
      ScheduleEventDecision.new(action: ScheduleEventAction::None, requested_run_state: nil)
    else
      ScheduleEventDecision.new(action: ScheduleEventAction::None, requested_run_state: nil)
    end
  end

  private def handle_run_state_request(requested : RunState) : ScheduleEventDecision
    if requested.print_report?
      return ScheduleEventDecision.new(action: ScheduleEventAction::PrintReport, requested_run_state: requested)
    end
    if requested.print_running_report?
      return ScheduleEventDecision.new(action: ScheduleEventAction::PrintRunningReport, requested_run_state: requested)
    end
    if requested.reload?
      return ScheduleEventDecision.new(action: ScheduleEventAction::Reload, requested_run_state: requested)
    end
    if !@run_state.normal?
      return ScheduleEventDecision.new(action: ScheduleEventAction::Invalid, requested_run_state: requested)
    end
    if requested.save?
      return ScheduleEventDecision.new(action: ScheduleEventAction::Save, requested_run_state: requested)
    end
    @run_state = requested
    @drain_state = DrainState::Draining
    ScheduleEventDecision.new(action: ScheduleEventAction::Transition, requested_run_state: requested)
  end
end

class ScheduleEventActions
  @schedule : Schedule
  @reporter : ScheduleReporter

  def initialize(@schedule : Schedule, @reporter : ScheduleReporter)
  end

  def apply(decision : ScheduleEventDecision, controller : ScheduleLoopController) : Bool
    case decision.action
    when .print_report?
      @schedule.print_report
      false
    when .print_running_report?
      @schedule.print_running_report
      false
    when .reload?
      @schedule.load
      false
    when .invalid?
      @reporter.invalid_transition(
        decision.requested_run_state.not_nil!,
        controller.run_state,
        controller.drain_state
      )
      false
    when .save?
      @schedule.save_state
      false
    when .transition?
      @reporter.run_state_changed(controller.run_state)
      false
    when .break_loop?
      true
    when .none?
      false
    else
      false
    end
  end
end

class ScheduleLoopRunner
  @schedule : Schedule
  @clock : Clock
  @loop_waiter : LoopWaiter

  def initialize(@schedule : Schedule, @clock : Clock, @loop_waiter : LoopWaiter)
  end

  def run(run_state_channel : Channel(RunState)? = nil)
    chan = Channel(Time).new
    events = Channel(TaskEvent).new
    controller = ScheduleLoopController.new(@clock)
    @schedule.load(true)
    if @schedule.autosave > 0.seconds
      spawn do
        autosave(run_state_channel, @schedule.autosave)
      end
      sleep(0.seconds)
    end
    while 1
      case controller.next_action(@schedule.running.size, @schedule.immediate)
      when .save_and_exit?
        @schedule.save_state
        exit
      when .exit?
        exit
      when .schedule_pass?
        @schedule.run_scheduling_pass(controller, chan, events)
      when .wait?
      end
      event = @loop_waiter.wait(run_state_channel, events, controller.shortest_timeout)
      if @schedule.process_schedule_event(event, controller)
        break
      end
    end
  end

  private def autosave(run_state_channel : Channel(RunState)?, wait_time : Time::Span)
    return unless run_state_channel
    while 1
      sleep(wait_time)
      run_state_channel.not_nil!.send(RunState::Save)
    end
  end
end
