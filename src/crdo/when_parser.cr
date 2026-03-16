struct TimeMatcher
  @month : Int32? = nil
  @month_day : Int32? = nil
  @weekday : Int32? = nil
  @hour : Int32? = nil
  @minute : Int32? = nil

  def initialize(@month, @month_day, @weekday, @hour, @minute)
    if @minute == nil && @hour == nil && @month_day == nil && @weekday == nil && @month == nil
      raise Exception.new("invalid TimeMatcher")
    end
  end

  def signature
    {
      month: @month,
      month_day: @month_day,
      weekday: @weekday,
      hour: @hour,
      minute: @minute,
    }.to_json
  end

  def get_interval
    case
    when @minute
      1.minutes
    when @hour
      1.hours
    else
      1.days
    end
  end

  def truncate(t)
    case
    when @minute
      t.at_beginning_of_minute
    when @hour
      t.at_beginning_of_hour
    else
      t.at_beginning_of_day
    end
  end

  def current_slot_start?(t)
    return nil unless match(t)
    truncate(t)
  end

  def find_next(t)
    interval = get_interval
    t += interval
    while !match(t)
      t += interval
    end
    truncate(t)
  end

  def match(t : Time)
    ret = true
    if @minute && t.minute != @minute.not_nil!
      ret = false
    end
    if @hour && t.hour != @hour.not_nil!
      ret = false
    end
    if @month_day && t.day != @month_day.not_nil!
      ret = false
    end
    if @weekday && t.day_of_week.to_i != @weekday.not_nil!
      ret = false
    end
    if @month && t.month != @month.not_nil!
      ret = false
    end
    ret
  end
end

def parse_when_token_values(full_text, token, short_day_of_week_names, short_month_names)
  parts = token.downcase.split(",")
  if parts.all? { |part| short_day_of_week_names.includes?(part) }
    return {
      "weekday",
      parts.map { |part| short_day_of_week_names.index!(part) + 1 },
    }
  end
  if parts.all? { |part| short_month_names.includes?(part) }
    return {
      "month",
      parts.map { |part| short_month_names.index!(part) + 1 },
    }
  end
  if parts.all? { |part| part.match(/^\d+:\d+$/) }
    values = parts.map do |part|
      hour, minute = part.split(":").map(&.to_i)
      if !(0..23).includes?(hour) || !(0..59).includes?(minute)
        raise Exception.new("invalid when #{full_text} token #{part} has invalid hour or minute")
      end
      {hour, minute}
    end
    return {"time", values}
  end
  if parts.all? { |part| part.match(/^\d+$/) }
    values = parts.map(&.to_i)
    values.each do |day|
      if !(1..31).includes?(day)
        raise Exception.new("invalid when #{full_text} token #{day} is not a valid month day")
      end
    end
    return {"month_day", values}
  end
  raise Exception.new("invalid when #{full_text} token #{token} mixes incompatible values")
end

def parse_when(txt)
  short_day_of_week_names = Time::DayOfWeek.names.map { |i| i.downcase[0..2] }
  short_month_names = %w(jan feb mar apr may jun jul aug sep oct nov dec)
  words = txt.split(/\s+/).reject(&.empty?)
  raise Exception.new("invalid when #{txt}") if words.empty?

  months = [] of Int32
  month_days = [] of Int32
  weekdays = [] of Int32
  times = [] of Tuple(Int32?, Int32?)

  words.each do |word|
    token_type, values = parse_when_token_values(txt, word, short_day_of_week_names, short_month_names)
    case token_type
    when "month"
      values.as(Array(Int32)).each { |value| months << value unless months.includes?(value) }
    when "month_day"
      values.as(Array(Int32)).each { |value| month_days << value unless month_days.includes?(value) }
    when "weekday"
      values.as(Array(Int32)).each { |value| weekdays << value unless weekdays.includes?(value) }
    when "time"
      values.as(Array(Tuple(Int32, Int32))).each do |value|
        expanded = {value[0].as(Int32?), value[1].as(Int32?)}
        times << expanded unless times.includes?(expanded)
      end
    else
      raise Exception.new("invalid when #{txt} token #{word}")
    end
  end

  times = [{nil, nil}] if times.empty?
  months_or_nil = months.empty? ? [nil] of Int32? : months.map(&.as(Int32?))
  month_days_or_nil = month_days.empty? ? [nil] of Int32? : month_days.map(&.as(Int32?))
  weekdays_or_nil = weekdays.empty? ? [nil] of Int32? : weekdays.map(&.as(Int32?))

  matchers = [] of TimeMatcher
  months_or_nil.each do |month|
    month_days_or_nil.each do |month_day|
      weekdays_or_nil.each do |weekday|
        times.each do |time|
          matchers << TimeMatcher.new(
            month: month,
            month_day: month_day,
            weekday: weekday,
            hour: time[0],
            minute: time[1])
        end
      end
    end
  end
  matchers
end
