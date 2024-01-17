require "json"
require "yaml"

struct TimeMatcher
@month : Int32? = nil
@day : Int32? = nil
@hour : Int32? = nil
@minute : Int32? = nil

def initialize(@month, @day, @hour, @minute)
if @minute==nil && @hour==nil && @day==nil && @month==nil
raise Exception.new("invalid TimeMatcher")
end
end

def find_next(t)
interval = case 
when @minute
1.minutes
when @hour
1.hours
when @day
1.days
when @month
1.months
else
raise Exception.new("invalid time matcher")
end
# add interval to t because we don't want to match on the current minute
t+=interval
while ! match(t)
t+=interval
end
t
end

def match(t : Time)
if @minute && t.minute != @minute.not_nil!
false
elsif @hour && t.hour != @hour.not_nil!
false
elsif @day && t.day != @day.not_nil!
false
elsif @month && t.month != @month.not_nil!
false
else
true
end
end

end


enum WaitReason
# no reason, go ahead
None
# task is already running
Running
# task has a task group and one of that groups members is running
Serial
# task depends on a task that has not completed successfully
Depend
# wait for specific time or time interval to pass
Wait
end

def parse_when(txt)
short_day_of_week_names=Time::DayOfWeek.names.map {|i| i.downcase[0..2] }
short_month_names=%w(jan feb mar apr may jun jul aug sep oct nov dec)
has_month=false
has_day_of_week=false
words=txt.split(" ")
month=nil
day=nil
hour=nil
minute=nil
words.each do |w|
if short_day_of_week_names.includes?(w)
if has_day_of_week
raise Exception.new("invalid when #{txt} token #{w} is duplicate instance of weekday")
end
day_of_week=short_day_of_week_names.index!(w)+1
has_day_of_week=true
elsif short_month_names.includes?(w)
if has_month
raise Exception.new("invalid when #{txt} token #{w} is duplicate instance of month")
end
month=short_month_names.index!(w)+1
has_month=true
elsif w.match(/[0-9]+:[0-9]+$/)
hour,minute=w.split(":").map &.to_i
else
raise Exception.new("invalid when #{txt} token #{w} is not month|weekday|hour:minute")
end
end #each
TimeMatcher.new(month: month, day: day, hour: hour, minute: minute)
end

def parse_time_span(txt)
t=txt.match(/(\d+)([smhd])/)
if ! t
raise Exception.new("invalid span #{txt}")
end
t=t.not_nil!
scale=t[2]
t=(t[1].to_i)
case scale
when "s"
t.seconds
when "m"
t.minutes
when "h"
t.hours
when "d"
t.days
else
raise Exception.new("invalid span #{txt} suffix #{t}")
end
end


class GlobalConfig
@error=false
@test = false
@mail : String? = nil
@workdir : String? = nil

getter test, error, mail

def workdir
@workdir.not_nil!
end

def initialize(data : YAML::Any)
data.as_h.each do |k,v|
case k.as_s
when "mail"
@mail=v.as_s
when "workdir"
@workdir=Path[v.as_s].expand(home: true).to_s
when "error"
@error=v.as_bool
when "test"
@test=v.as_bool
else
raise Exception.new("global config has invalid key #{k.as_s}")
end #case
end #each key
if ! @workdir
raise Exception.new("global config must specify workdir")
end
end #def

end


class Task
@commands=[] of String
@vars=Hash(String,String).new
@name : String
@when : TimeMatcher? = nil
@every : Time::Span? = nil
@group : String? = nil
@depends : String? = nil
@global : GlobalConfig
getter name, every, group, depends,commands, global

def when
@when
end

def initialize(@name : String, data : YAML::Any, @global : GlobalConfig)
data.as_h.each do |k,v|
case k
when "every"
@every=parse_time_span v.as_s
when "when"
@when = parse_when(v.as_s)
when "group"
@group=v.as_s
when "depends"
@depends=v.as_s
when "commands"
v.as_a.each do |c|
@commands << c.as_s
end #each command
when "vars"
v.as_h.each do |kk,vv|
@vars[kk.as_s]=vv.as_s
end #each var
else
raise Exception.new("task #{name} has invalid key #{k}")
end #case
end #each key
flag=0
flag+=1 if @every
flag+=1 if @when
if flag==0
raise Exception.new("task #{name} must have either `every` or `when` key")
end
if flag==2
raise Exception.new("task #{name} must have only one `every` or `when` key")
end
end

def hydrate_command(c)
@vars.each do |k,v|
c=c.gsub("$#{k}", v)
end
parts=Process.parse_arguments(c)
parts[0]=Path[parts[0]].expand(home: true, base: @global.workdir).to_s
parts
end

def verify
verify_commands
end

def verify_commands
@commands.each_with_index do |i, idx|
t=hydrate_command(i)
if ! File.executable?(t[0])
raise Exception.new("task #{@name}, command #{idx}, no path #{t[0]}")
end
end #each command
end #def

end #class


class Crontab
@tasks=[] of Task
@global : GlobalConfig
getter tasks, global

def initialize
t=YAML.parse File.read(Path["~/.crdo.yml"].expand(home: true))
@global=GlobalConfig.new t["global"]
keys=t.as_h.keys.reject {|i| i=="global" }
@tasks=keys.map {|key| Task.new(name: key.as_s, data: t[key], global: @global) }
end

def verify
verify_tasks
end

def verify_tasks
errs=[] of Exception
@tasks.each do |t|
begin
t.verify
rescue e
errs << e
end
end
if errs.size>0
raise Exception.new errs.map(&.to_s).join("\n")
end
check_dependencies
end

def check_dependencies
by_name=Hash(String,Task).new
@tasks.each do |task|
by_name[task.name]=task
end
seen=Set(String).new
@tasks.each do |task|
t=task
seen.clear
while t
seen << t.name
if t.depends && seen.includes?(t.depends.not_nil!)
raise Exception.new("task #{task.name} has a cyclical dependency of #{t.depends}")
end #if
if t.depends
t=by_name[t.depends.not_nil!]
else
t=nil
end # if
end #while
end #each
end #def

end #class


class ScheduleItem
@errors = [] of Exception
@schedule : Schedule
@task : Task
@last_start : Time? = nil
@last_stop : Time? = nil
@last_status = -1
@running = false
# have all this tasks dependencies run?
# if a task has dependents, (AKA children),
# and it runs successfully,
# it sets schedule.depends[name]=true for each child.
# After a child runs, it sets all schedule.depends entries to false.
# so we can verify that a task is runnable per dependency requirements by
# making sure no values in depends are false.
@depends=Hash(String,Bool).new
@sp : Process? = nil
getter depends

def task
@task
end

def partial_success?
@last_command_index<@task.commands.size
end

def initialize(@task, @schedule)
end

def running?
@running
end

def started(start_time : Time)
@running=true
@last_start=start_time
puts "running #{@task.name}"
end

def stopped(status : Int32, last_command_index : Int32, stop_time : Time)
@running=false
@last_status=status
@last_stop=stop_time
success=@last_status == 0
if @task.global.test && @task.global.error
success=false
end
if success
# let all dependents know we've run successfully
if @task.depends
child=@schedule[@task.depends.not_nil!]
child.depends[@task.name]=true
end #if dependency
# now that we have run,
# we require a new run of any tasks _we depend on
@depends.keys.each do |k|
@depends[k]=false
end #each parent
else # non-zero exit status
if @task.global.mail
args=[] of String
args+=["-s", "task #{@task.name} exitted #{@last_status}"]
fl=Dir.glob("cron_logs/#{@task.name}*")
fl.each do |f|
args+=["--attach",f]
end # each file
args << @task.global.mail.not_nil!
body=IO::Memory.new
body << "See attached files."
body.seek 0
Process.run(
command: "/usr/bin/mail",
args: args,
input: body
)
end #if mail
end # if/else success
end #def

# the scheduler calls started and stopped
# so it keeps a consistent view of tasks and their statuses.
def run(start_channel, events_channel)
start_channel.send(Time.local)
last_command=-1
rc=0
@task.commands.each_with_index do |c,idx|
last_command+=1
t=@task.hydrate_command(c)
begin
rc=run t,idx
rescue exc
rc=999
@errors << exc
end #rescue
break if rc!=0
end #each
events_channel.send({self, rc, last_command, Time.local})
end #def

def run(args : Array(String), idx : Int32)
if @schedule.test
args=args.clone
args.unshift "echo"
end
File.write("cron_logs/#{@task.name}.#{idx}.cmdline", args.to_json)
error_fh=File.open("cron_logs/#{@task.name}.#{idx}.stderr", "wb")
output_fh=File.open("cron_logs/#{@task.name}.#{idx}.stdout", "wb")
begin
@sp = Process.new(
command: args[0],
args: args[1..-1],
error: error_fh,
#Process::Redirect::Inherit,
output: output_fh,
#Process::Redirect::Inherit,
chdir: @task.global.workdir
)
ret=@sp.not_nil!.wait.exit_status
rescue e
error_fh << "\n#{e.inspect}"
ensure
error_fh.close
output_fh.close
end
ret.not_nil!
end

def should_run?
# don't run the same task in parallel
if @running
return {WaitReason::Running, @task.name, 0.seconds}
end
rr=@schedule.running.map &.task.name
# don't run a task in parallel with any other task in the same serial group
if @task.group && @schedule.running.any? {|i| i.task.group==@task.group }
return {WaitReason::Serial, @task.group.not_nil!, 0.seconds}
end
# don't run a task if it has a prerequisit task and that task has not been completed
if @task.depends && @depends.any? {|i| i==false }
return {WaitReason::Depend, @task.depends.not_nil!, 0.seconds}
end
if @task.when
return {WaitReason::Wait, "", @task.when.not_nil!.find_next(Time.local)-Time.local}
end
# run every x sec|min|hour|day
if @task.every
# if task hasn't been run before, then run
if ! @last_start
return {WaitReason::None, "", 0.seconds}
end
# if enough time has passed between the last time we started the process and the current time, run it
elapsed = Time.local-@last_start.not_nil!
if elapsed>=@task.every.not_nil!
return {WaitReason::None, "", 0.seconds}
end
# need to wait
return {WaitReason::Wait, "", (@task.every.not_nil!-elapsed)}
end #if every
raise Exception.new("task does not have every or when")
end #def

end #class


class Schedule
@schedule = [] of ScheduleItem
@test=false
property :test

def [](name : String)
@schedule.find! {|i| i.task.name==name }
end

def running
@schedule.select &.running?
end

def add_tasks(tasks)
tasks.each do |t|
@schedule << ScheduleItem.new(task: t, schedule: self)
end
@schedule.each do |parent|
children=@schedule.select {|i| i.depends==parent }
children.each do |c|
# mark each child as needing it's parent to complete a fresh run before it can run
c.depends[parent.task.name]=false
end #each child
end #each parent
end #def

def loop(filter=nil)
if filter
@schedule.select! {|i| i.task.name == filter }
end
reasons=[] of Tuple(ScheduleItem,Tuple(WaitReason, String, Time::Span))
chan=Channel(Time).new
events=Channel(Tuple(ScheduleItem, Int32, Int32, Time)).new
while 1
reasons.clear
@schedule.each do |i|
reason=i.should_run?
if reason[0].none?
spawn do
i.run chan, events
end
i.started chan.receive
else
reasons << {i,reason}
end #if
end #each
timeout_reasons = reasons.select {|i| i[1][0].wait? }
timeouts = timeout_reasons.map {|i| i[1][2] }
shortest_timeout = timeouts.size>0 ? timeouts.min : 1.hour
reasons.sort_by! do |i|
i[1][2]
end
reasons.each do |r|
puts "#{r[0].task.name} #{r[1][0].to_s} #{r[1][1]} #{r[1][2].total_seconds}"
end
puts "-----"
# wait on events from any task
#puts timeout_reasons
select
when x=events.receive
stopped(x)
next
when timeout(shortest_timeout)
next
end #select
end #while
end #def

def stopped(x)
x[0].stopped(status: x[1], last_command_index: x[2], stop_time: x[3])
end

end #class


class Crdo
@schedule : Schedule
getter schedule

def initialize
ct=Crontab.new
ct.verify
Dir.cd ct.global.workdir
Dir.mkdir_p "cron_logs"
@schedule=Schedule.new
@schedule.test=ct.global.test
@schedule.add_tasks ct.tasks
end

end


t=Crdo.new
t.schedule.loop ARGV[0]?
