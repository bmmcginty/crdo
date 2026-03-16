class CrontabVerifier
  def verify!(tasks : Array(Task))
    verify_tasks(tasks)
    check_dependencies(tasks)
  end

  def verify_tasks(tasks : Array(Task))
    errs = [] of Exception
    tasks.each do |task|
      begin
        task.verify
      rescue e
        errs << e
      end
    end
    if errs.size > 0
      raise Exception.new(errs.map(&.to_s).join("\n"))
    end
  end

  def check_dependencies(tasks : Array(Task))
    by_name = Hash(String, Task).new
    tasks.each do |task|
      by_name[task.name] = task
    end
    seen = Set(String).new
    tasks.each do |task|
      current = task
      seen.clear
      while current
        seen << current.name
        if current.parent && !by_name.has_key?(current.parent.not_nil!)
          raise Exception.new("task #{task.name} depends on missing parent #{current.parent}")
        end
        if current.parent && seen.includes?(current.parent.not_nil!)
          raise Exception.new("task #{task.name} has a cyclical dependency of #{current.parent}")
        end
        current = current.parent ? by_name[current.parent.not_nil!] : nil
      end
    end
  end
end
