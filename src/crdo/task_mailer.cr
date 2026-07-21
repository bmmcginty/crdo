class TaskMailer
  def send_mail(to, subject, body : IO | String | Nil, attach : Array(String)?) : MailDeliveryResult
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
    status = Process.run(
      command: "/usr/bin/mail",
      args: args,
      input: body
    )
    return MailDeliveryResult.new(success: true, message: "") if status.success?
    MailDeliveryResult.new(success: false, message: "mail exited #{status.exit_code}")
  rescue e
    MailDeliveryResult.new(success: false, message: e.message || e.inspect)
  end

  def notify_overtime(task : Task)
    send_mail(
      to: task.global.mail.not_nil!,
      subject: "Task #{task.name} now in overtime",
      body: nil,
      attach: nil)
  end

  def notify_failure(task : Task, last_status : Int32, log_dir : String) : MailDeliveryResult
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
