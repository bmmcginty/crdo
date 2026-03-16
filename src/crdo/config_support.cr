class CrontabSourceLoader
  @path : String

  def initialize(@path)
  end

  def load
    crdo_path = Path[@path].expand(home: true)
    if !File.exists?(crdo_path)
      STDERR.puts "config file not found: #{crdo_path}"
      exit 1
    end
    root = YAML.parse(File.read(crdo_path))
    global = GlobalConfig.new(root["global"])
    global.include_paths.each do |include_path|
      merge_include(root, crdo_path, include_path)
    end
    {global, root}
  end

  def merge_include(root : YAML::Any, crdo_path : Path, include_path : String)
    include_tasks = YAML.parse(File.read(Path[include_path].expand(base: File.dirname(crdo_path), home: true)))
    if include_tasks["global"]?
      raise Exception.new("include file #{include_path} has invalid `global` key")
    end
    include_tasks.as_h.each do |k, v|
      if root[k]?
        raise Exception.new("#{include_path}:#{k} conflicts with already existing task with same name")
      end
      root.as_h[k] = v
    end
  end
end

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
