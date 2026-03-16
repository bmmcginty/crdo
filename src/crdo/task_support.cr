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
      process.wait.exit_code
    rescue e
      error_fh << "\n#{e.inspect}"
      raise e
    ensure
      error_fh.close
      output_fh.close
    end
  end
end

class TaskMailer
  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?)
    body = case body
           when String
             IO::Memory.new(body)
           when Nil
             IO::Memory.new
           else
             body
           end
    args = [] of String
    args += ["-s", subject]
    if attach
      attach.each do |f|
        args += ["--attach", f]
      end
    end
    args << to
    body.seek(0)
    Process.run(
      command: "/usr/bin/mail",
      args: args,
      input: body
    )
  end

  def notify_overtime(task : Task)
    send_mail(
      to: task.global.mail.not_nil!,
      subject: "Task #{task.name} now in overtime",
      body: nil,
      attach: nil)
  end

  def notify_failure(task : Task, last_status : Int32, log_dir : String)
    subject = "task #{task.name} exitted #{last_status}"
    attachments = Dir.glob("#{log_dir}/*")
    body = IO::Memory.new
    if task.error_body
      body << task.error_body
      body << "\n"
    end
    body << "See attached files."
    send_mail(
      to: task.global.mail.not_nil!,
      subject: subject,
      attach: attachments,
      body: body)
  end
end
