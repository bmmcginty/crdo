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

  def scheduling_open?
    @run_state.normal? && @drain_state.none?
  end

  def note_running_count(running_count : Int32)
    if @drain_state.draining? && running_count == 0
      @drain_state = DrainState::Drained
    end
  end

  def should_save_before_exit?(immediate : Bool)
    @drain_state.drained? && @run_state.exit? && !immediate
  end

  def should_exit?
    @drain_state.drained? && @run_state.exit?
  end

  def update_reasons(reasons : Array(TaskWaitState))
    timeouts = reasons.select { |i| i[:reason].wait? }.map { |i| i[:time] }
    @shortest_timeout = timeouts.size > 0 ? timeouts.min : 1.hour
    reasons.sort_by! do |i|
      ({i[:reason], i[:time], i[:task].name})
    end
    @reasons = reasons
  end

  def handle_run_state_request(requested : RunState)
    if requested.print_report?
      return RunStateRequestAction::PrintReport
    end
    if requested.print_running_report?
      return RunStateRequestAction::PrintRunningReport
    end
    if requested.reload?
      return RunStateRequestAction::Reload
    end
    if !@run_state.normal?
      return RunStateRequestAction::Invalid
    end
    if requested.save?
      return RunStateRequestAction::Save
    end
    @run_state = requested
    @drain_state = DrainState::Draining
    RunStateRequestAction::Transition
  end

  def immediate_complete?(immediate : Bool, all_tasks_have_run_once : Bool)
    immediate && all_tasks_have_run_once
  end
end
