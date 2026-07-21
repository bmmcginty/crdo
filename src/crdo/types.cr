require "json"
require "option_parser"
require "yaml"

record TaskRunDecision,
  task : Task,
  reason : WaitReason,
  text : String,
  wait_time : Time::Span

record CliOptions,
  test : Bool,
  immediate : Bool,
  crontab : String,
  filter : Set(String)

record TaskStateSnapshot,
  last_start : Time?,
  last_stop : Time?,
  last_status : Int32

record TaskStoppedEvent,
  task_state : TaskState,
  status : Int32,
  last_command_index : Int32,
  stop_time : Time

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

enum WaitReason
  None
  AlreadyRunning
  Serial
  Depend
  Wait
  Exclusive
  Disabled
end

enum RuntimeMode
  Normal
  Exiting
  Done
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
  task_stopped : TaskStoppedEvent? do
  def self.timeout
    new(kind: SchedulerEventKind::Timeout, task_stopped: nil)
  end

  def self.reload_requested
    new(kind: SchedulerEventKind::ReloadRequested, task_stopped: nil)
  end

  def self.save_requested
    new(kind: SchedulerEventKind::SaveRequested, task_stopped: nil)
  end

  def self.exit_requested
    new(kind: SchedulerEventKind::ExitRequested, task_stopped: nil)
  end

  def self.print_report_requested
    new(kind: SchedulerEventKind::PrintReportRequested, task_stopped: nil)
  end

  def self.print_running_report_requested
    new(kind: SchedulerEventKind::PrintRunningReportRequested, task_stopped: nil)
  end

  def self.task_stopped(task_state : TaskState, status : Int32, last_command_index : Int32, stop_time : Time)
    new(
      kind: SchedulerEventKind::TaskStopped,
      task_stopped: TaskStoppedEvent.new(
        task_state: task_state,
        status: status,
        last_command_index: last_command_index,
        stop_time: stop_time))
  end
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
