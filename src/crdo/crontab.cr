class Crontab
  @tasks = [] of Task
  @global : GlobalConfig
  @verifier : CrontabVerifier
  getter tasks, global

  def initialize(path)
    @verifier = CrontabVerifier.new
    @global, source = CrontabSourceLoader.new(path).load
    keys = source.as_h.keys.reject { |i| i == "global" }
    @tasks = keys.map { |key| Task.new(name: key.as_s, data: source[key], global: @global) }
  end

  def verify
    @verifier.verify!(@tasks)
  end
end
