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
        @schedule.autosave(run_state_channel, @schedule.autosave)
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
end
