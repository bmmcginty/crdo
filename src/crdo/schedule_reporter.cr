class ScheduleReporter
  @clock : Clock
  @output : IO

  def initialize(@clock : Clock, @output : IO = STDOUT)
  end

  def next_task_wait(state : TaskState, decision : TaskRunDecision)
    if decision.reason.wait?
      next_time = @clock.now + decision.wait_time
      "#{format_time_span(decision.wait_time)} (#{next_time})"
    else
      next_time = state.next_scheduled_time(@clock.now)
      "#{format_time_span(next_time - @clock.now)} (#{next_time})"
    end
  end

  def print_running_report(schedule : Array(TaskState))
    running = schedule.select(&.running?)
    running.sort_by! { |i| i.task.name }
    running.each do |i|
      @output.puts "#{i.task.name}, #{i.run_time(@clock.now)}"
    end
    @output.puts "-----"
  end

  def print_report(reasons : Array(TaskRunDecision), mail_failures : Array(MailFailure) = [] of MailFailure)
    @output.puts "as of #{@clock.now}"
    reasons.each do |decision|
      state_text = decision.reason.none? || decision.reason.already_running? ? "running" : decision.reason.to_s
      @output.puts "#{decision.task.name}, #{state_text}: #{decision.text} #{format_time_span(decision.wait_time)}"
    end
    if mail_failures.size > 0
      @output.puts "mail failures:"
      mail_failures.each do |failure|
        @output.puts "#{failure.time} #{failure.task_name}: #{failure.message} #{failure.log_dir}"
      end
    end
    @output.puts "-----"
  end

  def started(task : TaskState, start_time : Time)
    @output.puts "start #{task.task.name} at #{start_time}"
  end

  def stopped(task : TaskState, status : Int32, next_wait : String)
    duration = if task.last_start && task.last_stop
                 task.last_stop.not_nil! - task.last_start.not_nil!
               else
                 0.seconds
               end
    @output.puts "stop #{task.task.name} rc=#{status} duration=#{format_time_span(duration)} next=#{next_wait}"
  end

  def run_state_changed(mode : RuntimeMode)
    @output.puts "run state #{mode}"
  end

  def invalid_transition(requested : SchedulerEventKind, current : RuntimeMode)
    @output.puts "requested run state #{requested} but currently have runtime mode #{current}"
  end
end
