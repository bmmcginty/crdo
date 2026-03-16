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

  def handle_event(event : ScheduleEvent, immediate : Bool, all_tasks_have_run_once : Bool = false) : ScheduleEventAction
    case event.kind
    when .run_state_request?
      handle_run_state_request(event.run_state.not_nil!)
    when .task_completed?
      immediate && all_tasks_have_run_once ? ScheduleEventAction::BreakLoop : ScheduleEventAction::None
    when .timeout?
      ScheduleEventAction::None
    else
      ScheduleEventAction::None
    end
  end

  private def handle_run_state_request(requested : RunState) : ScheduleEventAction
    if requested.print_report?
      return ScheduleEventAction::PrintReport
    end
    if requested.print_running_report?
      return ScheduleEventAction::PrintRunningReport
    end
    if requested.reload?
      return ScheduleEventAction::Reload
    end
    if !@run_state.normal?
      return ScheduleEventAction::Invalid
    end
    if requested.save?
      return ScheduleEventAction::Save
    end
    @run_state = requested
    @drain_state = DrainState::Draining
    ScheduleEventAction::Transition
  end
end
