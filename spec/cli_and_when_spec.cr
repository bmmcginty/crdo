require "./spec_helper"

describe "parse_cli" do
  it "parses file, now, test, and task filters" do
    options = parse_cli(["--file", "/tmp/example.yml", "--now", "--test", "task1", "task2"])

    options.crontab.should eq("/tmp/example.yml")
    options.immediate.should be_true
    options.test.should be_true
    options.filter.should eq(Set{"task1", "task2"})
  end
end

describe "parse_when" do
  it "supports weekday and time cartesian expansion" do
    matchers = parse_when("mon,tue 23:00,07:00")

    matchers.size.should eq(4)
    matchers.any? { |matcher| matcher.match(Time.local(2026, 3, 16, 23, 0, 0)) }.should be_true
    matchers.any? { |matcher| matcher.match(Time.local(2026, 3, 17, 7, 0, 0)) }.should be_true
  end

  it "supports month plus month-day plus time" do
    matchers = parse_when("may 3 23:00")

    matchers.size.should eq(1)
    matchers.first.match(Time.local(2026, 5, 3, 23, 0, 0)).should be_true
    matchers.first.match(Time.local(2026, 6, 3, 23, 0, 0)).should be_false
  end

  it "accepts arrays of when values at task level" do
    task = load_task(
      "a",
      "when:\n  - mon 10:00\n  - fri 11:00\ncommands:\n  - /bin/true\n"
    )

    task.when_specs.size.should eq(2)
  end

  it "finds day-granularity weekday schedules across spring DST without drift" do
    location = Time::Location.load("America/New_York")
    matcher = parse_when("mon").first
    before_spring_forward = Time.local(2026, 3, 7, 12, 0, 0, location: location)

    matcher.find_next(before_spring_forward).should eq(Time.local(2026, 3, 9, 0, 0, 0, location: location))
  end

  it "finds day-granularity weekday schedules across fall DST without drift" do
    location = Time::Location.load("America/New_York")
    matcher = parse_when("mon").first
    before_fall_back = Time.local(2026, 10, 31, 12, 0, 0, location: location)

    matcher.find_next(before_fall_back).should eq(Time.local(2026, 11, 2, 0, 0, 0, location: location))
  end
end
