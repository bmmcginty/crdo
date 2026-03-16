require "./spec_helper"

describe GlobalConfig do
  it "accepts include as a string or array" do
    cfg = load_global("workdir: .\ninclude: child.yml\n")

    cfg.include_paths.should eq(["child.yml"])
  end
end

describe Task do
  it "requires commands to be an array" do
    expect_raises(Exception, /commands must be an array/) do
      load_task(
        "a",
        "every: 1s\ncommands: /bin/true\n"
      )
    end
  end

  it "expands config vars in direct-exec commands" do
    task = load_task(
      "a",
      "every: 1s\nvars:\n  db: storiesonline\ncommands:\n  - ../bin/tool $db\n",
      "workdir: /tmp/work"
    )

    task.hydrate_command("../bin/tool $db").should eq(["/tmp/bin/tool", "storiesonline"])
  end

  it "fails validation for undefined vars in direct commands" do
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/echo $missing\n"
    )

    expect_raises(Exception, /unknown var missing/) do
      task.verify
    end
  end

  it "keeps shell opt-in behavior available" do
    task = load_task(
      "a",
      "every: 1s\ncommands:\n  - /bin/sh -c 'echo $HOME'\n"
    )

    task.verify.should be_nil
  end

  it "parses use_stop_time and error fields" do
    task = load_task(
      "a",
      "every: 1s\nuse_stop_time: true\nerror_body: fix it\nerror_command: /bin/true\ncommands:\n  - /bin/true\n"
    )

    task.use_stop_time.should be_true
    task.error_body.should eq("fix it")
    task.error_command.should eq("/bin/true")
  end
end

describe Crontab do
  it "resolves includes relative to the config file path" do
    dir = unique_tmpdir("crdo-include")
    write_yaml("#{dir}/child.yml", "b:\n  every: 1s\n  commands:\n    - /bin/true\n")
    root = write_yaml(
      "#{dir}/root.yml",
      "global:\n  workdir: .\n  include: child.yml\na:\n  every: 1s\n  commands:\n    - /bin/true\n"
    )

    crontab = Crontab.new(root)

    crontab.tasks.map(&.name).sort.should eq(["a", "b"])
  end

  it "fails cleanly on a missing parent" do
    dir = unique_tmpdir("crdo-parent")
    root = write_yaml(
      "#{dir}/root.yml",
      "global:\n  workdir: .\na:\n  every: 1s\n  parent: missing\n  commands:\n    - /bin/true\n"
    )

    expect_raises(Exception, /depends on missing parent missing/) do
      Crontab.new(root).verify
    end
  end
end
