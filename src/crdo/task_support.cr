class TaskProcessRunner
  def log_dn(task : Task, ts : Time)
    t = ts.to_s("%Y-%m-%d/%H-%M-%S")
    "#{task.global.workdir}/cron_logs/#{task.name}/#{t}"
  end

  def run(task : Task, args : Array(String), idx : Int32, start_time : Time, test : Bool) : Int32
    if test
      args = args.clone
      args.unshift("echo")
    end
    dn = log_dn(task, start_time)
    Dir.mkdir_p(dn)
    File.write("#{dn}/#{idx}.cmdline", args.to_json)
    error_fh = File.open("#{dn}/#{idx}.stderr", "wb")
    output_fh = File.open("#{dn}/#{idx}.stdout", "wb")
    begin
      process = Process.new(
        command: args[0],
        args: args[1..-1],
        error: error_fh,
        output: output_fh,
        chdir: task.global.workdir
      )
      process.wait.exit_code
    rescue e
      error_fh << "\n#{e.inspect}"
      raise e
    ensure
      error_fh.close
      output_fh.close
    end
  end
end

record TaskRunContext,
  running : Array(TaskState),
  immediate : Bool,
  filter : Set(String),
  now : Time

class TaskRunEligibilityEvaluator
  def evaluate(state : TaskState, context : TaskRunContext) : TaskWaitState
    task = state.task
    if state.retiring
      return TaskWaitState.new(task: task, reason: WaitReason::Disabled, text: "retiring", time: 0.seconds)
    end
    if task.disabled
      return TaskWaitState.new(task: task, reason: WaitReason::Disabled, text: task.name, time: 0.seconds)
    end
    if state.running?
      return TaskWaitState.new(task: task, reason: WaitReason::AlreadyRunning, text: task.name, time: state.run_time)
    end
    if task.group == "$exclusive" && context.running.size > 0
      return TaskWaitState.new(task: task, reason: WaitReason::Exclusive, text: "*", time: 0.seconds)
    end
    if task.group && context.running.any? { |i| i.task.group == task.group }
      return TaskWaitState.new(task: task, reason: WaitReason::Serial, text: task.group.not_nil!, time: 0.seconds)
    end
    if context.running.any? { |i| i.task.group == "$exclusive" }
      return TaskWaitState.new(task: task, reason: WaitReason::Serial, text: "$exclusive", time: 0.seconds)
    end
    if task.parent && !(context.immediate && context.filter.includes?(task.name)) && state.parent_status[task.parent.not_nil!] == false
      return TaskWaitState.new(task: task, reason: WaitReason::Depend, text: task.parent.not_nil!, time: 0.seconds)
    end
    if task.when_specs.size > 0
      if task.when_specs.any? { |matcher| (slot = matcher.current_slot_start?(context.now)) && (!state.last_start || state.last_start.not_nil! < slot.not_nil!) }
        return TaskWaitState.new(task: task, reason: WaitReason::None, text: "", time: 0.seconds)
      end
      next_time = task.when_specs.map { |matcher| matcher.find_next(context.now) }.min
      return TaskWaitState.new(task: task, reason: WaitReason::Wait, text: "", time: next_time - context.now)
    end
    if task.every
      if !state.last_start
        return TaskWaitState.new(task: task, reason: WaitReason::None, text: "", time: 0.seconds)
      end
      elapsed = if task.use_stop_time
                  context.now - (state.last_stop || state.last_start).not_nil!
                else
                  context.now - state.last_start.not_nil!
                end
      if elapsed < task.every.not_nil!
        return TaskWaitState.new(task: task, reason: WaitReason::Wait, text: "", time: (task.every.not_nil! - elapsed))
      end
      return TaskWaitState.new(task: task, reason: WaitReason::None, text: "", time: 0.seconds)
    end
    raise Exception.new("task does not have every or when")
  end
end

class TaskStopHandler
  @mailer : TaskMailer

  def initialize(@mailer = TaskMailer.new)
  end

  def handle(state : TaskState, schedule : Schedule)
    if success?(state)
      clear_parent_requirements(state)
      propagate_success(state, schedule)
    else
      clear_parent_requirements(state)
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

class TaskMailer
  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?)
    body = case body
           when String
             IO::Memory.new(body)
           when Nil
             IO::Memory.new
           else
             body
           end
    args = [] of String
    args += ["-s", subject]
    if attach
      attach.each do |f|
        args += ["--attach", f]
      end
    end
    args << to
    body.seek(0)
    Process.run(
      command: "/usr/bin/mail",
      args: args,
      input: body
    )
  end

  def notify_overtime(task : Task)
    send_mail(
      to: task.global.mail.not_nil!,
      subject: "Task #{task.name} now in overtime",
      body: nil,
      attach: nil)
  end

  def notify_failure(task : Task, last_status : Int32, log_dir : String)
    subject = "task #{task.name} exitted #{last_status}"
    attachments = Dir.glob("#{log_dir}/*")
    body = IO::Memory.new
    if task.error_body
      body << task.error_body
      body << "\n"
    end
    body << "See attached files."
    send_mail(
      to: task.global.mail.not_nil!,
      subject: subject,
      attach: attachments,
      body: body)
  end
end
