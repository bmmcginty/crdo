class TaskStopHandler
  @mailer : TaskMailer

  def initialize(@mailer = TaskMailer.new)
  end

  def handle(state : TaskState, schedule : Schedule)
    clear_parent_requirements(state)
    if success?(state)
      propagate_success(state, schedule)
    else
      run_error_command(state.task)
      notify_failure(state)
    end
  end

  def success?(state : TaskState)
    success = state.success?
    if state.task.global.test && state.task.global.error
      success = false
    end
    success
  end

  def clear_parent_requirements(state : TaskState)
    state.parent_status.keys.each do |k|
      state.parent_status[k] = false
    end
  end

  def propagate_success(state : TaskState, schedule : Schedule)
    children = schedule.select { |i| i.task.parent == state.task.name }
    children.each do |child|
      child.parent_status[state.task.name] = true
    end
  end

  def run_error_command(task : Task)
    return unless task.error_command
    spawn do
      ec = task.hydrate_command(task.error_command.not_nil!)
      Process.run(command: ec[0], args: ec[1..-1], chdir: task.global.workdir)
    end
    sleep 0.seconds
  end

  def notify_failure(state : TaskState)
    return unless state.task.global.mail
    @mailer.notify_failure(state.task, state.last_status, state.log_dn(state.last_start.as(Time)))
  end
end
