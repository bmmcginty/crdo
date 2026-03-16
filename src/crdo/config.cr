class GlobalConfig
  @error = false
  @test = false
  @ignore_overtime = false
  @mail : String? = nil
  @autosave : Time::Span = 600.seconds
  @workdir : String? = nil
  @include_paths = [] of String
  @print_report = true

  getter test, error, mail, ignore_overtime, include_paths, print_report
  getter! workdir, autosave

  def initialize(data : YAML::Any)
    data.as_h.each do |k, v|
      case k.as_s
      when "include"
        @include_paths.concat(yaml_string_array(v))
      when "autosave"
        @autosave = v.as_i.seconds
      when "print_report"
        @print_report = v.as_bool
      when "ignore_overtime"
        @ignore_overtime = v.as_bool
      when "mail"
        @mail = v.as_s
      when "workdir"
        @workdir = Path[v.as_s].expand(home: true).to_s
      when "error"
        @error = v.as_bool
      when "test"
        @test = v.as_bool
      else
        raise Exception.new("global config has invalid key #{k.as_s}")
      end
    end
    if !@workdir
      raise Exception.new("global config must specify workdir")
    end
  end
end

class Crontab
  @tasks = [] of Task
  @global : GlobalConfig
  getter tasks, global

  def initialize(path)
    crdo_path = Path[path].expand(home: true)
    if !File.exists?(crdo_path)
      STDERR.puts "config file not found: #{crdo_path}"
      exit 1
    end
    t = YAML.parse(File.read(crdo_path))
    @global = GlobalConfig.new(t["global"])
    @global.include_paths.each do |include_path|
      include_tasks = YAML.parse(File.read(Path[include_path].expand(base: File.dirname(crdo_path), home: true)))
      if include_tasks["global"]?
        raise Exception.new("include file #{include_path} has invalid `global` key")
      end
      include_tasks.as_h.each do |k, v|
        if t[k]?
          raise Exception.new("#{include_path}:#{k} conflicts with already existing task with same name")
        end
        t.as_h[k] = v
      end
    end
    keys = t.as_h.keys.reject { |i| i == "global" }
    @tasks = keys.map { |key| Task.new(name: key.as_s, data: t[key], global: @global) }
  end

  def verify
    verify_tasks
  end

  def verify_tasks
    errs = [] of Exception
    @tasks.each do |t|
      begin
        t.verify
      rescue e
        errs << e
      end
    end
    if errs.size > 0
      raise Exception.new(errs.map(&.to_s).join("\n"))
    end
    check_dependencies
  end

  def check_dependencies
    by_name = Hash(String, Task).new
    @tasks.each do |task|
      by_name[task.name] = task
    end
    seen = Set(String).new
    @tasks.each do |task|
      t = task
      seen.clear
      while t
        seen << t.name
        if t.parent && !by_name.has_key?(t.parent.not_nil!)
          raise Exception.new("task #{task.name} depends on missing parent #{t.parent}")
        end
        if t.parent && seen.includes?(t.parent.not_nil!)
          raise Exception.new("task #{task.name} has a cyclical dependency of #{t.parent}")
        end
        if t.parent
          t = by_name[t.parent.not_nil!]
        else
          t = nil
        end
      end
    end
  end
end
