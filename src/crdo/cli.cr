def parse_cli(args = ARGV)
  test = false
  immediate = false
  ct = "~/.crdo.yml"
  filter = Array(String).new.to_set
  parser = OptionParser.new do |parser|
    parser.on(
      "-h",
      "--help",
      "show this help") do
      puts parser
      exit
    end
    parser.on(
      "--file name",
      "location of crdo file"
    ) do |name|
      ct = name
    end
    parser.on(
      "--now",
      "run a single task without reading or writing task state"
    ) do
      immediate = true
    end
    parser.on("--test",
      "prefix all commands with echo") do
      test = true
    end
    parser.unknown_args do |args|
      filter = args.to_set
    end
  end
  parser.parse(args)
  CliOptions.new(test: test, immediate: immediate, crontab: ct, filter: filter)
end

def main(args = ARGV)
  options = parse_cli(args)
  events = Channel(SchedulerEvent).new(16)
  Signal::HUP.trap do
    enqueue_scheduler_event(events, SchedulerEvent.reload_requested)
  end
  Signal::INT.trap do
    enqueue_scheduler_event(events, SchedulerEvent.exit_requested)
  end
  Signal::USR1.trap do
    enqueue_scheduler_event(events, SchedulerEvent.print_report_requested)
  end
  Signal::USR2.trap do
    enqueue_scheduler_event(events, SchedulerEvent.print_running_report_requested)
  end
  schedule = ScheduleState.new(test: options.test, immediate: options.immediate, filter: options.filter, crontab: options.crontab)
  puts "crdo running with pid #{Process.pid},#{options.immediate ? " immediate" : ""} #{options.test ? "test" : "normal"} mode"
  ScheduleRuntime.new(schedule, schedule.clock).run(events)
end

def enqueue_scheduler_event(events : Channel(SchedulerEvent), event : SchedulerEvent)
  select
  when events.send(event)
  else
  end
end
