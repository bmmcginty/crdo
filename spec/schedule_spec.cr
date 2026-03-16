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

describe TaskRunEligibilityEvaluator do
  it "uses the supplied context time for every schedules" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    state = TaskState.new(
      task: load_task("a", "every: 10s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    state.apply_snapshot(
      TaskStateSnapshot.new(
        last_start: Time.local - 8.seconds,
        last_stop: nil,
        last_status: 0)
    )

    result = TaskRunEligibilityEvaluator.new.evaluate(
      state,
      TaskRunContext.new(
        running: [] of TaskState,
        immediate: false,
        filter: Set(String).new,
        now: Time.local))

    result[:reason].wait?.should be_true
  end

  it "lets immediate filtered tasks bypass parent gating in pure evaluation" do
    schedule = Schedule.new(test: false, immediate: true, filter: Set{"child"}, crontab: "/tmp/unused.yml")
    state = TaskState.new(
      task: load_task("child", "every: 1d\nparent: parent\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    state.parent_status["parent"] = false

    result = TaskRunEligibilityEvaluator.new.evaluate(
      state,
      TaskRunContext.new(
        running: [] of TaskState,
        immediate: true,
        filter: Set{"child"},
        now: Time.local))

    result[:reason].none?.should be_true
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

describe ScheduleReloadPlanner do
  it "plans keep, replace, defer, and removed-running retirement without mutating state" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")

    unchanged = TaskState.new(
      task: load_task("same", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    changed_running = TaskState.new(
      task: load_task("changed", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    removed_running = TaskState.new(
      task: load_task("removed", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )

    changed_running.started(Time.local)
    removed_running.started(Time.local)

    incoming = [
      load_task("same", "every: 1s\ncommands:\n  - /bin/true\n"),
      load_task("changed", "every: 2s\ncommands:\n  - /bin/true\n"),
      load_task("fresh", "every: 1s\ncommands:\n  - /bin/true\n"),
    ]

    plan = ScheduleReloadPlanner.new([unchanged, changed_running, removed_running], incoming).plan

    same_entry = plan.entries.find! { |entry| entry.task.name == "same" }
    same_entry.keep_current.should be_true
    same_entry.current.should eq(unchanged)

    changed_entry = plan.entries.find! { |entry| entry.task.name == "changed" }
    changed_entry.retire_current.should be_true
    plan.deferred_tasks["changed"].name.should eq("changed")

    fresh_entry = plan.entries.find! { |entry| entry.task.name == "fresh" }
    fresh_entry.current.should be_nil
    fresh_entry.preserve_state.should be_false

    plan.retiring_removed.should eq([removed_running])
    changed_running.retiring.should be_false
    removed_running.retiring.should be_false
  end
end

describe ScheduleLoopController do
  it "tracks run-state transitions, invalid requests, and exit conditions" do
    controller = ScheduleLoopController.new

    controller.handle_run_state_request(RunState::Exit).transition?.should be_true
    controller.run_state.exit?.should be_true
    controller.drain_state.draining?.should be_true

    controller.handle_run_state_request(RunState::Save).invalid?.should be_true
    controller.note_running_count(0)
    controller.should_save_before_exit?(false).should be_true
    controller.should_exit?.should be_true
  end

  it "sorts wait reasons and tracks the shortest timeout" do
    controller = ScheduleLoopController.new
    a = TaskWaitState.new(
      task: load_task("a", "every: 1s\ncommands:\n  - /bin/true\n"),
      reason: WaitReason::Wait,
      text: "",
      time: 5.seconds
    )
    b = TaskWaitState.new(
      task: load_task("b", "every: 1s\ncommands:\n  - /bin/true\n"),
      reason: WaitReason::AlreadyRunning,
      text: "",
      time: 1.seconds
    )
    c = TaskWaitState.new(
      task: load_task("c", "every: 1s\ncommands:\n  - /bin/true\n"),
      reason: WaitReason::Wait,
      text: "",
      time: 2.seconds
    )

    controller.update_reasons([a, b, c])

    controller.shortest_timeout.should eq(2.seconds)
    controller.reasons.map { |reason| reason[:task].name }.should eq(["b", "c", "a"])
  end
end
