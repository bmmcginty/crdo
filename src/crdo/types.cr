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

alias TaskEvent = Tuple(TaskState, Int32, Int32, Time)

record MailDeliveryResult,
  success : Bool,
  message : String

record MailFailure,
  task_name : String,
  log_dir : String,
  message : String,
  time : Time

enum WhenForwardPolicy
  After
  Skip
end

enum WhenBackwardPolicy
  Once
  Repeat
end

record WhenPolicy,
  forward : WhenForwardPolicy,
  backward : WhenBackwardPolicy

struct WhenPolicy
  def to_json(json : JSON::Builder)
    json.object do
      json.field "forward", forward.to_s.downcase
      json.field "backward", backward.to_s.downcase
    end
  end
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

enum RuntimeState
  Normal
  Exit
end

enum SchedulerEventKind
  Timeout
  TaskStopped
  ReloadRequested
  SaveRequested
  ExitRequested
  PrintReportRequested
  PrintRunningReportRequested
end

record SchedulerEvent,
  kind : SchedulerEventKind,
  task_event : TaskEvent?

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

def parse_when_policy(value : YAML::Any)
  if value.raw == true
    return WhenPolicy.new(
      forward: WhenForwardPolicy::After,
      backward: WhenBackwardPolicy::Once)
  end
  if value.raw == false
    return nil
  end
  policy = value.as_h
  forward = case policy["forward"]?.try(&.as_s?)
            when nil
              WhenForwardPolicy::Skip
            when "after"
              WhenForwardPolicy::After
            when "skip"
              WhenForwardPolicy::Skip
            else
              raise Exception.new("invalid when_policy forward #{policy["forward"]}")
            end
  backward = case policy["backward"]?.try(&.as_s?)
             when nil
               WhenBackwardPolicy::Once
             when "once"
               WhenBackwardPolicy::Once
             when "repeat"
               WhenBackwardPolicy::Repeat
             else
               raise Exception.new("invalid when_policy backward #{policy["backward"]}")
             end
  WhenPolicy.new(forward: forward, backward: backward)
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
