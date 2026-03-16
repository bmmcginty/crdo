class Task
  @commands = [] of String
  @vars = Hash(String, String).new
  @error_body : String? = nil
  @error_command : String? = nil
  @name : String
  @when = [] of TimeMatcher
  @every : Time::Span? = nil
  @group : String? = nil
  @parent : String? = nil
  @when_policy : WhenPolicy? = nil
  @global : GlobalConfig
  @disabled = false
  @use_stop_time = false
  getter name, every, group, parent, use_stop_time, commands, global, disabled, error_body, error_command, vars, when_policy

  def when_specs
    @when
  end

  def signature
    {
      name: @name,
      commands: @commands,
      vars: @vars.to_a.sort_by(&.[0]),
      error_body: @error_body,
      error_command: @error_command,
      when: @when.map(&.signature).sort,
      every_seconds: @every.try(&.total_seconds),
      when_policy: @when_policy,
      group: @group,
      parent: @parent,
      disabled: @disabled,
      use_stop_time: @use_stop_time,
      global: {
        workdir: @global.workdir,
        mail: @global.mail,
        ignore_overtime: @global.ignore_overtime,
        error: @global.error,
        test: @global.test,
      },
    }.to_json
  end

  def initialize(@name : String, data : YAML::Any, @global : GlobalConfig)
    data.as_h.each do |k, v|
      case k
      when "every"
        @every = parse_time_span(v.as_s)
      when "use_stop_time"
        @use_stop_time = v.as_bool
      when "when"
        yaml_string_array(v).each do |value|
          @when.concat(parse_when(value))
        end
      when "when_policy"
        @when_policy = parse_when_policy(v)
      when "error_body"
        @error_body = v.as_s
      when "error_command"
        @error_command = v.as_s
      when "group"
        @group = v.as_s
      when "parent"
        @parent = v.as_s
      when "disabled"
        @disabled = v.as_bool
      when "commands"
        if !v.raw.is_a?(Array)
          raise Exception.new("task #{name} commands must be an array")
        end
        v.as_a.each do |c|
          @commands << c.as_s
        end
      when "vars"
        v.as_h.each do |kk, vv|
          @vars[kk.as_s] = vv.as_s
        end
      else
        raise Exception.new("task #{name} has invalid key #{k}")
      end
    end
    flag = 0
    flag += 1 if @every
    flag += 1 if @when.size > 0
    if flag == 0
      raise Exception.new("task #{name} must have either `every` or `when` key")
    end
    if flag == 2
      raise Exception.new("task #{name} must have only one `every` or `when` key")
    end
    if data["use_stop_time"]? && !@every
      raise Exception.new("task #{name} can only use `use_stop_time` with `every`")
    end
    if data["when_policy"]? && @when.empty?
      raise Exception.new("task #{name} can only use `when_policy` with `when`")
    end
  end

  def shell_command?(c)
    parts = Process.parse_arguments(c)
    parts[0] == "/bin/sh" && parts[1]? == "-c"
  rescue
    false
  end

  def hydrate_command(c)
    @vars.each do |k, v|
      c = c.gsub("$#{k}", v)
    end
    parts = Process.parse_arguments(c)
    parts[0] = Path[parts[0]].expand(home: true, base: @global.workdir).to_s
    parts
  end

  def verify_command_vars(c : String, label : String)
    return if shell_command?(c)
    c.scan(/\$([A-Za-z_][A-Za-z0-9_]*)/) do |match|
      var_name = match[1]
      if !@vars.has_key?(var_name)
        raise Exception.new("task #{@name}, #{label}, unknown var #{var_name}")
      end
    end
  end

  def verify
    verify_commands
    if @error_command
      verify_command_vars(@error_command.not_nil!, "error command")
      t = hydrate_command(@error_command.not_nil!)
      if !File::Info.executable?(t[0])
        raise Exception.new("task #{@name}, error command, no path #{t[0]}")
      end
    end
  end

  def verify_commands
    @commands.each_with_index do |i, idx|
      verify_command_vars(i, "command #{idx}")
      t = hydrate_command(i)
      if !File::Info.executable?(t[0])
        raise Exception.new("task #{@name}, command #{idx}, no path #{t[0]}")
      end
    end
  end
end
