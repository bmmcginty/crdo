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
