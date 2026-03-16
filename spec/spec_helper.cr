require "spec"
require "../crdo"

def unique_tmpdir(prefix = "crdo-spec")
  path = "/tmp/#{prefix}-#{Process.pid}-#{Time.utc.to_unix_ms}-#{Random.rand(1_000_000)}"
  Dir.mkdir_p(path)
  path
end

def write_yaml(path : String, text : String)
  File.write(path, text)
  path
end

def load_global(text : String)
  GlobalConfig.new(YAML.parse(text))
end

def load_task(name : String, text : String, global_text = "workdir: .")
  Task.new(name: name, data: YAML.parse(text), global: load_global(global_text))
end
