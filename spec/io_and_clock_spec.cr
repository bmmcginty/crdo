require "./spec_helper"

class RecordingMailer < TaskMailer
  getter deliveries = [] of NamedTuple(
    to: String,
    subject: String,
    body: String,
    attach: Array(String)?)

  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?)
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
      to: to.as(String),
      subject: subject.as(String),
      body: body_text,
      attach: attach,
    }
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
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    state = TaskState.new(
      task: load_task("a", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    state.apply_snapshot(TaskStateSnapshot.new(last_start: clock.now, last_stop: nil, last_status: 0))

    state.should_run?[:reason].wait?.should be_true
    clock.now = clock.now + 1.second
    state.should_run?[:reason].none?.should be_true
  end

  it "supports every 1m without waiting on wall clock" do
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    state = TaskState.new(
      task: load_task("a", "every: 1m\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    state.apply_snapshot(TaskStateSnapshot.new(last_start: clock.now, last_stop: nil, last_status: 0))

    state.should_run?[:reason].wait?.should be_true
    clock.now = clock.now + 1.minute
    state.should_run?[:reason].none?.should be_true
  end
end
