class ScheduleEventApplier
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
