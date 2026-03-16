class GlobalConfig
  @error = false
  @test = false
  @ignore_overtime = false
  @mail : String? = nil
  @autosave : Time::Span = 600.seconds
  @workdir : String? = nil
  @include_paths = [] of String
  @print_report = true

  getter test, error, mail, ignore_overtime, include_paths, print_report
  getter! workdir, autosave

  def initialize(data : YAML::Any)
    data.as_h.each do |k, v|
      case k.as_s
      when "include"
        @include_paths.concat(yaml_string_array(v))
      when "autosave"
        @autosave = v.as_i.seconds
      when "print_report"
        @print_report = v.as_bool
      when "ignore_overtime"
        @ignore_overtime = v.as_bool
      when "mail"
        @mail = v.as_s
      when "workdir"
        @workdir = Path[v.as_s].expand(home: true).to_s
      when "error"
        @error = v.as_bool
      when "test"
        @test = v.as_bool
      else
        raise Exception.new("global config has invalid key #{k.as_s}")
      end
    end
    if !@workdir
      raise Exception.new("global config must specify workdir")
    end
  end
end
