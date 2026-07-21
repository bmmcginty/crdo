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
  events = Channel(SchedulerEvent).new
  Signal::HUP.trap do
    events.send(SchedulerEvent.new(kind: SchedulerEventKind::ReloadRequested, task_stopped: nil))
  end
  Signal::INT.trap do
    events.send(SchedulerEvent.new(kind: SchedulerEventKind::ExitRequested, task_stopped: nil))
  end
  Signal::USR1.trap do
    events.send(SchedulerEvent.new(kind: SchedulerEventKind::PrintReportRequested, task_stopped: nil))
  end
  Signal::USR2.trap do
    events.send(SchedulerEvent.new(kind: SchedulerEventKind::PrintRunningReportRequested, task_stopped: nil))
  end
  t = ScheduleState.new(test: options.test, immediate: options.immediate, filter: options.filter, crontab: options.crontab)
  puts "crdo running with pid #{Process.pid},#{options.immediate ? " immediate" : ""} #{options.test ? "test" : "normal"} mode"
  t.loop(events)
end
