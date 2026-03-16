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
