class TaskProcessRunner
  def log_dn(task : Task, ts : Time)
    t = ts.to_s("%Y-%m-%d/%H-%M-%S")
    "#{task.global.workdir}/cron_logs/#{task.name}/#{t}"
  end

  def run(task : Task, args : Array(String), idx : Int32, start_time : Time, test : Bool) : Int32
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
      process = Process.new(
        command: args[0],
        args: args[1..-1],
        error: error_fh,
        output: output_fh,
        chdir: task.global.workdir
      )
      rc = process.wait.exit_code
      File.write("#{dn}/#{idx}.rc", "#{rc}\n")
      rc
    rescue e
      error_fh << "\n#{e.inspect}"
      File.write("#{dn}/#{idx}.rc", "999\n")
      raise e
    ensure
      error_fh.close
      output_fh.close
    end
  end
end
