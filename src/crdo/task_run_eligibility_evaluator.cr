record TaskRunContext,
  running : Array(TaskState),
  immediate : Bool,
  filter : Set(String),
  previous_now : Time?,
  now : Time

class TaskRunEligibilityEvaluator
  def evaluate(state : TaskState, context : TaskRunContext) : TaskRunDecision
    task = state.task
    if state.retiring
      return TaskRunDecision.new(task: task, reason: WaitReason::Disabled, text: "retiring", wait_time: 0.seconds)
    end
    if task.disabled
      return TaskRunDecision.new(task: task, reason: WaitReason::Disabled, text: task.name, wait_time: 0.seconds)
    end
    if state.running?
      return TaskRunDecision.new(task: task, reason: WaitReason::AlreadyRunning, text: task.name, wait_time: state.run_time(context.now))
    end
    if task.group == "$exclusive" && context.running.size > 0
      return TaskRunDecision.new(task: task, reason: WaitReason::Exclusive, text: "*", wait_time: 0.seconds)
    end
    if task.group && context.running.any? { |i| i.task.group == task.group }
      return TaskRunDecision.new(task: task, reason: WaitReason::Serial, text: task.group.not_nil!, wait_time: 0.seconds)
    end
    if context.running.any? { |i| i.task.group == "$exclusive" }
      return TaskRunDecision.new(task: task, reason: WaitReason::Serial, text: "$exclusive", wait_time: 0.seconds)
    end
    if task.parent && !(context.immediate && context.filter.includes?(task.name)) && state.parent_status[task.parent.not_nil!] == false
      return TaskRunDecision.new(task: task, reason: WaitReason::Depend, text: task.parent.not_nil!, wait_time: 0.seconds)
    end
    if task.when_specs.size > 0
      if task.when_specs.any? { |matcher| (slot = matcher.current_slot_start?(context.now)) && slot_runnable?(task, matcher, state, slot.not_nil!) }
        return TaskRunDecision.new(task: task, reason: WaitReason::None, text: "", wait_time: 0.seconds)
      end
      if task.when_policy.try(&.forward.after?) && context.previous_now && context.previous_now.not_nil! < context.now
        if task.when_specs.any? { |matcher| missed_slot_runnable?(task, matcher, state, context.previous_now.not_nil!, context.now) }
          return TaskRunDecision.new(task: task, reason: WaitReason::None, text: "", wait_time: 0.seconds)
        end
      end
      next_time = task.when_specs.map { |matcher| matcher.find_next(context.now) }.min
      return TaskRunDecision.new(task: task, reason: WaitReason::Wait, text: "", wait_time: next_time - context.now)
    end
    if task.every
      if !state.last_start
        return TaskRunDecision.new(task: task, reason: WaitReason::None, text: "", wait_time: 0.seconds)
      end
      elapsed = if task.use_stop_time
                  context.now - (state.last_stop || state.last_start).not_nil!
                else
                  context.now - state.last_start.not_nil!
                end
      if elapsed < task.every.not_nil!
        return TaskRunDecision.new(task: task, reason: WaitReason::Wait, text: "", wait_time: (task.every.not_nil! - elapsed))
      end
      return TaskRunDecision.new(task: task, reason: WaitReason::None, text: "", wait_time: 0.seconds)
    end
    raise Exception.new("task does not have every or when")
  end

  def slot_runnable?(task : Task, matcher : TimeMatcher, state : TaskState, slot : Time)
    return true unless state.last_start
    return false unless state.last_start.not_nil! < slot
    if task.when_policy.try(&.backward.once?) && matcher.slot_key(state.last_start.not_nil!) == matcher.slot_key(slot)
      return false
    end
    true
  end

  def missed_slot_runnable?(task : Task, matcher : TimeMatcher, state : TaskState, previous_now : Time, now : Time)
    candidate = matcher.find_next(previous_now - matcher.get_interval)
    while candidate <= now
      if candidate > previous_now && slot_runnable?(task, matcher, state, candidate)
        return true
      end
      candidate = matcher.find_next(candidate)
    end
    false
  end
end
