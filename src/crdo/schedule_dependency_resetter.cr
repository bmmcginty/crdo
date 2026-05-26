class ScheduleDependencyResetter
  def reset(task_states : Array(TaskState))
    task_states.each do |parent|
      children = task_states.select { |state| state.task.parent == parent.task.name }
      children.each do |child|
        child.parent_status[parent.task.name] = false
      end
    end
  end
end
