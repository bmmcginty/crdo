class TaskProcessRunner
  TIMEOUT_EXIT_CODE = 124
  KILL_GRACE_TIME   = 1.second
  SETSID            = "/usr/bin/setsid"
  @process : Process? = nil

  def log_dn(task : Task, ts : Time)
    t = ts.to_s("%Y-%m-%d/%H-%M-%S")
    "#{task.global.workdir}/cron_logs/#{task.name}/#{t}"
  end

  def run(task : Task, args : Array(String), idx : Int32, start_time : Time, test : Bool, timeout : Time::Span? = nil) : Int32
    if test
      args = args.clone
      args.unshift("echo")
    end
    dn = log_dn(task, start_time)
    Dir.mkdir_p(dn)
    File.write("#{dn}/#{idx}.cmdline", args.to_json)
    error_fh = File.open("#{dn}/#{idx}.stderr", "wb")
    output_fh = File.open("#{dn}/#{idx}.stdout", "wb")
    begin
      process_args = process_group_args(args)
      process = Process.new(
        command: process_args[0],
        args: process_args[1..-1],
        error: error_fh,
        output: output_fh,
        chdir: task.global.workdir
      )
      @process = process
      rc = wait_for_process(process, timeout, error_fh)
      File.write("#{dn}/#{idx}.rc", "#{rc}\n")
      rc
    rescue e
      error_fh << "\n#{e.inspect}"
      File.write("#{dn}/#{idx}.rc", "999\n")
      raise e
    ensure
      error_fh.close
      output_fh.close
      @process = nil
    end
  end

  def terminate_running
    process = @process
    return unless process

    terminate_process(process, nil)
  end

  private def wait_for_process(process : Process, wait_time : Time::Span?, error_fh : IO) : Int32
    return process.wait.exit_code unless wait_time

    done = Channel(Process::Status).new(1)
    spawn do
      done.send(process.wait)
    end

    select
    when status = done.receive
      status.exit_code
    when timeout(wait_time.not_nil!)
      error_fh << "\ncrdo: command timed out after #{format_time_span(wait_time.not_nil!)}\n"
      terminate_process(process, done)
      TIMEOUT_EXIT_CODE
    end
  end

  private def terminate_process(process : Process, done : Channel(Process::Status)?)
    signal_process_group(process, Signal::TERM)
    return unless done

    select
    when done.not_nil!.receive
    when timeout(KILL_GRACE_TIME)
      signal_process_group(process, Signal::KILL)
      done.not_nil!.receive
    end
  rescue
  end

  private def process_group_args(args : Array(String)) : Array(String)
    return args unless File::Info.executable?(SETSID)

    [SETSID] + args
  end

  private def signal_process_group(process : Process, signal : Signal)
    if File::Info.executable?(SETSID)
      Process.signal(signal, -process.pid.to_i)
    else
      process.signal(signal)
    end
  end
end
