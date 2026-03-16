require "./spec_helper"

def write_schedule_config(path : String, body : String)
  write_yaml(
    path,
    "global:\n  workdir: .\n#{body}"
  )
end

describe TaskState do
  it "marks disabled tasks as disabled" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    task = load_task(
      "a",
      "every: 1s\ndisabled: true\ncommands:\n  - /bin/true\n"
    )

    state = TaskState.new(task: task, schedule: schedule)
    state.should_run?[:reason].disabled?.should be_true
  end

  it "enforces serial groups and exclusive groups" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    a = TaskState.new(
      task: load_task("a", "every: 1s\ngroup: g\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    b = TaskState.new(
      task: load_task("b", "every: 1s\ngroup: g\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    ex = TaskState.new(
      task: load_task("ex", "every: 1s\ngroup: $exclusive\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )

    schedule.add_tasks([a.task, b.task, ex.task])
    schedule["a"].started(Time.local)

    schedule["b"].should_run?[:reason].serial?.should be_true
    schedule["ex"].should_run?[:reason].exclusive?.should be_true
  end

  it "uses stop time when requested" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    task = load_task(
      "a",
      "every: 10s\nuse_stop_time: true\ncommands:\n  - /bin/true\n"
    )
    state = TaskState.new(task: task, schedule: schedule)
    state.apply_snapshot(
      TaskStateSnapshot.new(
        last_start: Time.local - 30.seconds,
        last_stop: Time.local - 5.seconds,
        last_status: 0)
    )

    state.should_run?[:reason].wait?.should be_true
  end
end

describe Schedule do
  it "lets --now ignore parent gating for the filtered task" do
    dir = unique_tmpdir("crdo-now")
    path = write_schedule_config(
      "#{dir}/root.yml",
      "parent:\n  every: 1d\n  commands:\n    - /bin/true\nchild:\n  every: 1d\n  parent: parent\n  commands:\n    - /bin/true\n"
    )

    schedule = Schedule.new(test: false, immediate: true, filter: Set{"child"}, crontab: path)
    schedule.load(true)

    schedule["child"].should_run?[:reason].none?.should be_true
  end

  it "writes version 2 state and reads legacy state" do
    dir = unique_tmpdir("crdo-state")
    path = write_schedule_config(
      "#{dir}/root.yml",
      "a:\n  every: 1d\n  commands:\n    - /bin/true\n"
    )

    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: path)
    schedule.load(true)
    schedule["a"].apply_snapshot(TaskStateSnapshot.new(last_start: Time.local - 1.minute, last_stop: Time.local, last_status: 0))
    schedule.save_state

    saved = JSON.parse(File.read("#{path}.state"))
    saved["version"].as_i.should eq(2)

    File.write(
      "#{path}.state",
      [
        {
          name: "a",
          last_start_ms: (Time.local - 2.minutes).to_utc.to_unix_ms,
          last_stop_ms: (Time.local - 1.minute).to_utc.to_unix_ms,
          last_status: 0,
        },
      ].to_json
    )

    reloaded = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: path)
    reloaded.load(true)
    reloaded["a"].last_status.should eq(0)
  end

  it "keeps unchanged running tasks during reload and loads new unrelated tasks" do
    dir = unique_tmpdir("crdo-reload")
    path = write_schedule_config(
      "#{dir}/root.yml",
      "task1:\n  every: 1d\n  commands:\n    - /bin/true\n"
    )

    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: path)
    schedule.load(true)
    original = schedule["task1"]
    original.started(Time.local)

    write_schedule_config(
      path,
      "task1:\n  every: 1d\n  commands:\n    - /bin/true\ntask2:\n  every: 1d\n  commands:\n    - /bin/true\n"
    )

    schedule.load

    schedule["task1"].object_id.should eq(original.object_id)
    schedule["task1"].running?.should be_true
    schedule["task2"]?.should_not be_nil
  end
end
