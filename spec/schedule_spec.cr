require "./spec_helper"

class StopLoopWait < Exception
end

class RecordingLoopWaiter < LoopWaiter
  getter waits = [] of Time::Span

  def wait(run_state_channel : Channel(RunState)?, events : Channel(TaskEvent), wait_time : Time::Span) : ScheduleEvent
    @waits << wait_time
    raise StopLoopWait.new("stop after recording wait")
  end
end

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

  it "uses the injected schedule clock for should_run timing" do
    clock = FakeClock.new(Time.local)
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    task = load_task(
      "a",
      "every: 10s\ncommands:\n  - /bin/true\n"
    )
    state = TaskState.new(task: task, schedule: schedule)
    state.apply_snapshot(
      TaskStateSnapshot.new(
        last_start: clock.now - 8.seconds,
        last_stop: nil,
        last_status: 0)
    )

    state.should_run?[:reason].wait?.should be_true
    clock.now = clock.now + 3.seconds
    state.should_run?[:reason].none?.should be_true
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
        previous_now: nil,
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
        previous_now: nil,
        now: Time.local))

    result[:reason].none?.should be_true
  end
end

describe "when_policy" do
  it "runs after a missed forward jump when configured to after" do
    evaluator = TaskRunEligibilityEvaluator.new
    state = TaskState.new(
      task: load_task("a", "when: 01:00\nwhen_policy: true\ncommands:\n  - /bin/true\n"),
      schedule: Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    )

    result = evaluator.evaluate(
      state,
      TaskRunContext.new(
        running: [] of TaskState,
        immediate: false,
        filter: Set(String).new,
        previous_now: Time.local(2026, 3, 16, 0, 59, 0),
        now: Time.local(2026, 3, 16, 1, 1, 0)))

    result[:reason].none?.should be_true
  end

  it "skips a missed forward jump when configured to skip" do
    evaluator = TaskRunEligibilityEvaluator.new
    state = TaskState.new(
      task: load_task("a", "when: 01:00\nwhen_policy:\n  forward: skip\n  backward: once\ncommands:\n  - /bin/true\n"),
      schedule: Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    )

    result = evaluator.evaluate(
      state,
      TaskRunContext.new(
        running: [] of TaskState,
        immediate: false,
        filter: Set(String).new,
        previous_now: Time.local(2026, 3, 16, 0, 59, 0),
        now: Time.local(2026, 3, 16, 1, 1, 0)))

    result[:reason].wait?.should be_true
  end

  it "suppresses a repeated backward slot when configured to once" do
    evaluator = TaskRunEligibilityEvaluator.new
    state = TaskState.new(
      task: load_task("a", "when: 01:00\nwhen_policy: true\ncommands:\n  - /bin/true\n"),
      schedule: Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    )
    location = Time::Location.load("America/New_York")
    first_slot = Time.utc(2026, 11, 1, 5, 0, 0).in(location)
    second_slot = Time.utc(2026, 11, 1, 6, 0, 0).in(location)
    state.apply_snapshot(TaskStateSnapshot.new(
      last_start: first_slot,
      last_stop: nil,
      last_status: 0
    ))
    matcher = state.task.when_specs.first

    matcher.slot_key(first_slot).should eq(matcher.slot_key(second_slot))
    evaluator.slot_runnable?(state.task, matcher, state, second_slot).should be_false
  end

  it "allows a repeated backward slot when configured to repeat" do
    evaluator = TaskRunEligibilityEvaluator.new
    state = TaskState.new(
      task: load_task("a", "when: 01:00\nwhen_policy:\n  forward: skip\n  backward: repeat\ncommands:\n  - /bin/true\n"),
      schedule: Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    )
    location = Time::Location.load("America/New_York")
    first_slot = Time.utc(2026, 11, 1, 5, 0, 0).in(location)
    second_slot = Time.utc(2026, 11, 1, 6, 0, 0).in(location)
    state.apply_snapshot(TaskStateSnapshot.new(
      last_start: first_slot,
      last_stop: nil,
      last_status: 0
    ))
    matcher = state.task.when_specs.first

    matcher.slot_key(first_slot).should eq(matcher.slot_key(second_slot))
    evaluator.slot_runnable?(state.task, matcher, state, second_slot).should be_true
  end
end

describe TaskStopHandler do
  it "clears dependency requirements and propagates success to children" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    parent = TaskState.new(
      task: load_task("parent", "every: 1d\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    child = TaskState.new(
      task: load_task("child", "every: 1d\nparent: parent\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )

    schedule.add_tasks([parent.task, child.task])
    schedule["parent"].parent_status["upstream"] = true
    schedule["parent"].started(Time.local - 1.second)
    schedule["parent"].stopped(status: 0, last_command_index: 0, stop_time: Time.local)

    schedule["parent"].parent_status["upstream"].should be_false
    schedule["child"].parent_status["parent"].should be_true
  end

  it "treats test+error mode as a failure for dependency propagation" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml")
    parent = TaskState.new(
      task: load_task("parent", "every: 1d\ncommands:\n  - /bin/true\n", "workdir: .\ntest: true\nerror: true"),
      schedule: schedule
    )
    child = TaskState.new(
      task: load_task("child", "every: 1d\nparent: parent\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )

    schedule.add_tasks([parent.task, child.task])
    schedule["parent"].started(Time.local - 1.second)
    schedule["parent"].stopped(status: 0, last_command_index: 0, stop_time: Time.local)

    schedule["child"].parent_status["parent"]?.should_not eq(true)
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

  it "does not partially apply state when any entry is invalid for this schedule" do
    dir = unique_tmpdir("crdo-state-atomic")
    path = write_schedule_config(
      "#{dir}/root.yml",
      "a:\n  every: 1d\n  commands:\n    - /bin/true\n"
    )

    File.write(
      "#{path}.state",
      [
        {
          name: "a",
          last_start_ms: (Time.local - 2.minutes).to_utc.to_unix_ms,
          last_stop_ms: (Time.local - 1.minute).to_utc.to_unix_ms,
          last_status: 0,
        },
        {
          name: "missing",
          last_start_ms: Time.local.to_utc.to_unix_ms,
          last_stop_ms: Time.local.to_utc.to_unix_ms,
          last_status: 0,
        },
      ].to_json
    )

    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: path)
    schedule.load(true)

    schedule["a"].last_status.should eq(-1)
    schedule["a"].last_start.should be_nil
    schedule["a"].last_stop.should be_nil
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

  it "requests the computed wait timeout instead of busy-spinning for every schedules" do
    dir = unique_tmpdir("crdo-loop-wait")
    path = write_schedule_config(
      "#{dir}/root.yml",
      "a:\n  every: 1m\n  commands:\n    - /bin/true\n"
    )
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    waiter = RecordingLoopWaiter.new
    File.write(
      "#{path}.state",
      {
        version: 2,
        tasks: [
          {
            name: "a",
            last_start_ms: clock.now.to_utc.to_unix_ms,
            last_stop_ms: nil,
            last_status: 0,
          },
        ],
      }.to_json
    )
    schedule = Schedule.new(
      test: false,
      immediate: false,
      filter: Set(String).new,
      crontab: path,
      clock: clock,
      loop_waiter: waiter
    )

    expect_raises(StopLoopWait) do
      schedule.loop
    end

    waiter.waits.first.should eq(1.minute)
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
    controller = ScheduleLoopController.new(FakeClock.new(Time.local))
    exit_event = ScheduleEvent.new(kind: ScheduleEventKind::RunStateRequest, run_state: RunState::Exit, task_event: nil)
    save_event = ScheduleEvent.new(kind: ScheduleEventKind::RunStateRequest, run_state: RunState::Save, task_event: nil)

    controller.handle_event(exit_event, immediate: false).transition?.should be_true
    controller.run_state.exit?.should be_true
    controller.drain_state.draining?.should be_true

    controller.handle_event(save_event, immediate: false).invalid?.should be_true
    controller.next_action(0, false).save_and_exit?.should be_true
    controller.next_action(0, true).exit?.should be_true
  end

  it "sorts wait reasons and tracks the shortest timeout" do
    controller = ScheduleLoopController.new(FakeClock.new(Time.local))
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

  it "returns schedule or wait based on current loop state" do
    controller = ScheduleLoopController.new(FakeClock.new(Time.local))
    exit_event = ScheduleEvent.new(kind: ScheduleEventKind::RunStateRequest, run_state: RunState::Exit, task_event: nil)

    controller.next_action(0, false).schedule_pass?.should be_true
    controller.handle_event(exit_event, immediate: false)
    controller.next_action(1, false).wait?.should be_true
  end

  it "returns break-loop for immediate completion events" do
    controller = ScheduleLoopController.new(FakeClock.new(Time.local))
    task = load_task("a", "every: 1s\ncommands:\n  - /bin/true\n")
    schedule = Schedule.new(test: false, immediate: true, filter: Set(String).new, crontab: "/tmp/unused.yml")
    state = TaskState.new(task: task, schedule: schedule)
    event = ScheduleEvent.new(
      kind: ScheduleEventKind::TaskCompleted,
      run_state: nil,
      task_event: {state, 0, 0, Time.local}
    )

    controller.handle_event(event, immediate: true, all_tasks_have_run_once: true).break_loop?.should be_true
    controller.handle_event(event, immediate: true, all_tasks_have_run_once: false).none?.should be_true
  end
end

describe SchedulePassPlanner do
  it "plans start actions only for unfiltered runnable tasks" do
    schedule = Schedule.new(test: false, immediate: false, filter: Set{"a"}, crontab: "/tmp/unused.yml")
    a = TaskState.new(
      task: load_task("a", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    b = TaskState.new(
      task: load_task("b", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )

    planner = SchedulePassPlanner.new
    decisions = planner.plan([a, b], Set{"a"})

    decisions.map(&.task_state.task.name).should eq(["a"])
    decisions.first.action.start_task?.should be_true
  end

  it "plans overtime notifications for overdue running tasks" do
    clock = FakeClock.new(Time.local(2026, 3, 16, 12, 0, 0))
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    state = TaskState.new(
      task: load_task("a", "every: 1s\ncommands:\n  - /bin/true\n"),
      schedule: schedule
    )
    state.started(clock.now - 2.seconds)

    decision = SchedulePassPlanner.new.plan([state], Set(String).new).first

    decision.wait_state[:reason].already_running?.should be_true
    decision.action.notify_overtime?.should be_true
  end
end

describe ScheduleReporter do
  it "uses the injected clock when formatting wait times" do
    clock = FakeClock.new(Time.local(2026, 3, 16, 10, 0, 0))
    reporter = ScheduleReporter.new(clock)
    schedule = Schedule.new(test: false, immediate: false, filter: Set(String).new, crontab: "/tmp/unused.yml", clock: clock)
    task = load_task("a", "every: 10s\ncommands:\n  - /bin/true\n")
    state = TaskState.new(task: task, schedule: schedule)
    state.apply_snapshot(
      TaskStateSnapshot.new(
        last_start: clock.now - 8.seconds,
        last_stop: nil,
        last_status: 0)
    )

    reporter.next_task_wait(state).should contain("00:02")
    reporter.next_task_wait(state).should contain("2026-03-16 10:00:02")
  end
end
