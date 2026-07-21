require "./spec_helper"

class RecordingMailer < TaskMailer
  getter deliveries = [] of NamedTuple(
    to: String,
    subject: String,
    body: String,
    attach: Array(String)?)

  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?) : MailDeliveryResult
    body_text = case body
                when String
                  body
                when Nil
                  ""
                else
                  body.rewind
                  body.gets_to_end
                end
    @deliveries << {
      to:      to.as(String),
      subject: subject.as(String),
      body:    body_text,
      attach:  attach,
    }
    MailDeliveryResult.new(success: true, message: "")
  end
end

class FailingMailer < TaskMailer
  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?) : MailDeliveryResult
    MailDeliveryResult.new(success: false, message: "mail unavailable")
  end
end

describe TaskProcessRunner do
  it "writes command logs and captures stdout" do
    dir = unique_tmpdir("crdo-runner")
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/echo hello\n",
      "workdir: #{dir}"
    )
    runner = TaskProcessRunner.new
    start_time = Time.local(2026, 3, 16, 12, 0, 0)

    rc = runner.run(task, ["/bin/echo", "hello"], 0, start_time, false)

    rc.should eq(0)
    log_dir = runner.log_dn(task, start_time)
    JSON.parse(File.read("#{log_dir}/0.cmdline")).as_a.map(&.as_s).should eq(["/bin/echo", "hello"])
    File.read("#{log_dir}/0.rc").should eq("0\n")
    File.read("#{log_dir}/0.stdout").should contain("hello")
    File.read("#{log_dir}/0.stderr").should eq("")
  end

  it "prefixes commands with echo in test mode" do
    dir = unique_tmpdir("crdo-runner-test")
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/echo hello\n",
      "workdir: #{dir}"
    )
    runner = TaskProcessRunner.new
    start_time = Time.local(2026, 3, 16, 12, 0, 1)

    runner.run(task, ["/bin/echo", "hello"], 0, start_time, true)

    log_dir = runner.log_dn(task, start_time)
    JSON.parse(File.read("#{log_dir}/0.cmdline")).as_a.map(&.as_s).should eq(["echo", "/bin/echo", "hello"])
    File.read("#{log_dir}/0.stdout").should contain("/bin/echo hello")
  end
end

describe TaskState do
  it "sends failure mail when a later command exits non-zero" do
    dir = unique_tmpdir("crdo-second-command-failure")
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/true\n  - /bin/sh -c 'exit 23'\n",
      "workdir: #{dir}\nmail: ops@example.com"
    )
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = ScheduleState.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock, output: IO::Memory.new)
    mailer = RecordingMailer.new
    state = TaskState.new(task: task)
    runtime = ScheduleRuntime.new(schedule, clock, mailer)
    events = Channel(SchedulerEvent).new(1)
    start_time = clock.now

    state.started(start_time)
    state.run(start_time, events, false, clock)
    event = events.receive.task_stopped.not_nil!
    runtime.handle_task_stopped(event)

    state.last_status.should eq(23)
    event.last_command_index.should eq(1)
    log_dir = state.log_dn(state.last_start.not_nil!)
    File.read("#{log_dir}/0.rc").should eq("0\n")
    File.read("#{log_dir}/1.rc").should eq("23\n")
    mailer.deliveries.size.should eq(1)
    mailer.deliveries.first[:subject].should eq("task a exitted 23")
  end

  it "writes mailfail and records report state when failure mail cannot be delivered" do
    dir = unique_tmpdir("crdo-mailfail")
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/sh -c 'exit 23'\n",
      "workdir: #{dir}\nmail: ops@example.com"
    )
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = ScheduleState.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock, output: IO::Memory.new)
    state = TaskState.new(task: task)
    runtime = ScheduleRuntime.new(schedule, clock, FailingMailer.new)
    events = Channel(SchedulerEvent).new(1)
    start_time = clock.now

    state.started(start_time)
    state.run(start_time, events, false, clock)
    event = events.receive.task_stopped.not_nil!
    runtime.handle_task_stopped(event)

    log_dir = state.log_dn(state.last_start.not_nil!)
    File.read("#{log_dir}/mailfail").should contain("mail unavailable")
    schedule.mail_failures.size.should eq(1)
    schedule.mail_failures.first.task_name.should eq("a")
    schedule.mail_failures.first.log_dir.should eq(log_dir)
    schedule.mail_failures.first.message.should eq("mail unavailable")
  end
end

describe TaskMailer do
  it "builds overtime mail content" do
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/true\n",
      "workdir: .\nmail: ops@example.com"
    )
    mailer = RecordingMailer.new

    mailer.notify_overtime(task)

    mailer.deliveries.size.should eq(1)
    mailer.deliveries.first[:to].should eq("ops@example.com")
    mailer.deliveries.first[:subject].should eq("Task a now in overtime")
  end

  it "builds failure mail with body and attachments" do
    dir = unique_tmpdir("crdo-mail")
    File.write("#{dir}/0.stdout", "stdout")
    File.write("#{dir}/0.stderr", "stderr")
    task = load_task(
      "a",
      "every: 1s\nerror_body: fix it\ncommands:\n  - /bin/true\n",
      "workdir: .\nmail: ops@example.com"
    )
    mailer = RecordingMailer.new

    mailer.notify_failure(task, 23, dir)

    mailer.deliveries.size.should eq(1)
    mailer.deliveries.first[:subject].should eq("task a exitted 23")
    mailer.deliveries.first[:body].should contain("fix it")
    mailer.deliveries.first[:attach].not_nil!.sort.should eq(Dir.glob("#{dir}/*").sort)
  end
end

describe "every with fake clock" do
  it "supports every 1s without waiting on wall clock" do
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = ScheduleState.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    state = TaskState.new(task: load_task("a", "every: 1s\ncommands:\n  - /bin/true\n"))
    state.apply_snapshot(TaskStateSnapshot.new(last_start: clock.now, last_stop: nil, last_status: 0))

    schedule.task_wait_state(state)[:reason].wait?.should be_true
    clock.now = clock.now + 1.second
    schedule.task_wait_state(state)[:reason].none?.should be_true
  end

  it "supports every 1m without waiting on wall clock" do
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = ScheduleState.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    state = TaskState.new(task: load_task("a", "every: 1m\ncommands:\n  - /bin/true\n"))
    state.apply_snapshot(TaskStateSnapshot.new(last_start: clock.now, last_stop: nil, last_status: 0))

    schedule.task_wait_state(state)[:reason].wait?.should be_true
    clock.now = clock.now + 1.minute
    schedule.task_wait_state(state)[:reason].none?.should be_true
  end
end
