require "json"
require "option_parser"
require "yaml"

alias TaskWaitState = NamedTuple(
  task: Task,
  reason: WaitReason,
  text: String,
  time: Time::Span)

record CliOptions,
  test : Bool,
  immediate : Bool,
  crontab : String,
  filter : Set(String)

record TaskStateSnapshot,
  last_start : Time?,
  last_stop : Time?,
  last_status : Int32

enum RunState
  Normal
  Reload
  Save
  Exit
  PrintReport
  PrintRunningReport
end

enum DrainState
  None
  Draining
  Drained
end

enum WaitReason
  None
  AlreadyRunning
  Serial
  Depend
  Wait
  Exclusive
  Disabled
end

def format_time_span(t)
  "#{((t.days * 24) + t.hours).to_s.rjust(2, '0')}:#{t.minutes.to_s.rjust(2, '0')}:#{t.seconds.to_s.rjust(2, '0')}".gsub(/^00?:/, "")
end

def yaml_string_array(value : YAML::Any)
  if value.raw.is_a?(Array)
    value.as_a.map(&.as_s)
  else
    [value.as_s]
  end
end

def parse_time_span(txt)
  t = txt.match(/(\d+)([smhd])/)
  if !t
    raise Exception.new("invalid span #{txt}")
  end
  t = t.not_nil!
  scale = t[2]
  t = t[1].to_i
  case scale
  when "s"
    t.seconds
  when "m"
    t.minutes
  when "h"
    t.hours
  when "d"
    t.days
  else
    raise Exception.new("invalid span #{txt} suffix #{t}")
  end
end
