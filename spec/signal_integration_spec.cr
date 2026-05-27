require "./spec_helper"

def wait_for_file(path : String, timeout_ms : Int32 = 3000) : Bool
  deadline = Time.instant + timeout_ms.milliseconds
  until Time.instant >= deadline
    return true if File.exists?(path)
    sleep(10.milliseconds)
  end
  File.exists?(path)
end

def wait_for_log_line(path : String, needle : String, timeout_ms : Int32 = 3000) : Bool
  deadline = Time.instant + timeout_ms.milliseconds
  until Time.instant >= deadline
    if File.exists?(path)
      begin
        return true if File.read(path).includes?(needle)
      rescue
      end
    end
    sleep(10.milliseconds)
  end
  File.exists?(path) && File.read(path).includes?(needle)
end

def pid_alive?(pid : Int64) : Bool
  Process.run(
    "kill",
    ["-0", pid.to_s],
    output: Process::Redirect::Close,
    error: Process::Redirect::Close
  ).success?
end

describe "signal wiring" do
  it "handles USR1/USR2/INT through run-state channel" do
    dir = unique_tmpdir("crdo-signal")
    bin_path = "#{dir}/crdo-bin"
    out_path = "#{dir}/stdout.log"
    err_path = "#{dir}/stderr.log"
    config_path = write_yaml(
      "#{dir}/root.yml",
      "global:\n" \
      "  workdir: #{dir}\n" \
      "  autosave: 3600\n" \
      "  print_report: true\n" \
      "job1:\n" \
      "  every: 1h\n" \
      "  commands:\n" \
      "    - /bin/true\n"
    )

    build_status = Process.run(
      "crystal",
      ["build", "crdo.cr", "-o", bin_path],
      output: Process::Redirect::Close,
      error: Process::Redirect::Close
    )
    build_status.success?.should be_true

    process = Process.new(
      bin_path,
      ["--file", config_path],
      output: File.new(out_path, "w"),
      error: File.new(err_path, "w")
    )

    wait_for_file(out_path).should be_true
    wait_for_log_line(out_path, "crdo running with pid", 5000).should be_true

    pid_alive?(process.pid).should be_true
    Process.signal(Signal::USR1, process.pid)
    sleep(100.milliseconds)
    pid_alive?(process.pid).should be_true
    Process.signal(Signal::USR2, process.pid)
    sleep(100.milliseconds)
    pid_alive?(process.pid).should be_true
    Process.signal(Signal::INT, process.pid)
    status = process.wait
    status.should_not be_nil

    output = File.read(out_path)
    output.includes?("as of ").should be_true
    output.includes?("-----").should be_true
    output.includes?("run state Exit").should be_true
  end
end
