require "json"
require "option_parser"
require "yaml"

enum RunState
Normal
Reload
Save
Exit
end


enum DrainState
None
Draining
Drained
end


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
# task is disabled in crontab
Disabled
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


# stores global configuration
class GlobalConfig
@error=false
@test = false
@mail : String? = nil
@autosave : Time::Span = 600.seconds
@workdir : String? = nil
@include_paths = [] of String

getter test, error, mail, include_paths
getter! workdir, autosave

def initialize(data : YAML::Any)
data.as_h.each do |k,v|
case k.as_s
when "include"
@include_paths.concat(v.as_a.map &.as_s)
when "autosave"
@autosave=v.as_i.seconds
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


# stores settings for a single task
class Task
@commands=[] of String
@vars=Hash(String,String).new
@error_body : String? = nil
@error_command : String? = nil
@name : String
@when : TimeMatcher? = nil
@every : Time::Span? = nil
@group : String? = nil
@parent : String? = nil
@global : GlobalConfig
@disabled=false
getter name, every, group, parent, commands, global, disabled, error_body, error_command

# we need this as a def
# because `when` in a getter macro raises a syntax error
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
when "error_body"
@error_body=v.as_s
when "error_command"
@error_command=v.as_s
when "group"
@group=v.as_s
when "parent"
@parent=v.as_s
when "disabled"
@disabled=v.as_bool
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
if @error_command
t=hydrate_command(@error_command.not_nil!)
if ! File.executable?(t[0])
raise Exception.new("task #{@name}, error command, no path #{t[0]}")
end # executable
end # if error command
end # def

def verify_commands
@commands.each_with_index do |i, idx|
t=hydrate_command(i)
if ! File.executable?(t[0])
raise Exception.new("task #{@name}, command #{idx}, no path #{t[0]}")
end
end #each command
end #def

end #class


# parses and validates crdo file
class Crontab
@tasks=[] of Task
@global : GlobalConfig
getter tasks, global

def initialize(path)
crdo_path=Path[path].expand(home: true)
t=YAML.parse File.read(crdo_path)
@global=GlobalConfig.new t["global"]
@global.include_paths.each do |include_path|
include_tasks=YAML.parse File.read(Path[include_path].expand(base: File.dirname(crdo_path), home: true))
if include_tasks["global"]?
raise Exception.new("include file #{include_path} has invalid `global` key")
end
include_tasks.as_h.each do |k,v|
if t[k]?
raise Exception.new("#{include_path}:#{k} conflicts with already existing task with same name")
end
t.as_h[k]=v
end
end
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
if t.parent && seen.includes?(t.parent.not_nil!)
raise Exception.new("task #{task.name} has a cyclical dependency of #{t.parent}")
end #if
if t.parent
t=by_name[t.parent.not_nil!]
else
t=nil
end # if
end #while
end #each
end #def

end #class


# each task must have a schedule item, which holds task state.
# Tasks come from the crontab, while TaskState is loaded from a save file or created fresh on each run.
class TaskState
@errors = [] of Exception
@schedule : Schedule
@task : Task
@current_start : Time? = nil
@last_start : Time? = nil
@last_stop : Time? = nil
@last_status = -1
@running = false
# each child keeps a log of parent_name->has_successfully run status.
# each parent sets this flag to true for each of it's children upon a successful run.
# each child clears that flag for each of it's parents, after it itself runs.
# so we can verify that a task is runnable per dependency requirements by
# making sure no values in parent_status are false.
@parent_status=Hash(String,Bool).new
@sp : Process? = nil
getter parent_status

def initialize(@task, @schedule)
end

def to_json(json : JSON::Builder)
json.object do
json.field "name", @task.name
json.field "last_status", @last_status
json.field "last_stop_ms", (@last_stop ? @last_stop.not_nil!.to_utc.to_unix_ms : nil)
json.field "last_start_ms", (@last_start ? @last_start.not_nil!.to_utc.to_unix_ms : nil)
end
end

def set_state(data : JSON::Any)
@last_start = if t=data["last_start"]?.try(&.as_i64?)
Time.unix(t).to_local
elsif t=data["last_start_ms"]?.try(&.as_i64?)
Time.unix_ms(t).to_local
else
nil
end
@last_stop = if t=data["last_stop"]?.try(&.as_i64?)
Time.unix(t).to_local
elsif t=data["last_stop_ms"]?.try(&.as_i64?)
Time.unix_ms(t).to_local
else
nil
end
@last_status = if t=data["last_status"]?.try(&.as_i?)
t
else
nil
end
@parent_status.clear
end

def has_run_successfully_since?(ts : Time)
(! running?) && success? && @last_start && @last_stop && @last_stop.not_nil!>=@last_start.not_nil! && @last_start.not_nil!>=ts
end

def task
@task
end

def running?
@running
end

def started(start_time : Time)
@running=true
@current_start=start_time
puts "running #{@task.name}"
end

def success?
@last_status==0
end

def log_dn(ts)
ts.to_s("cron_logs/#{@task.name}/%Y-%m-%d/%H-%M-%S")
end

def stopped(status : Int32, last_command_index : Int32, stop_time : Time)
@running=false
@last_start=@current_start
@last_status=status
@last_stop=stop_time
success = success?
if @task.global.test && @task.global.error
success=false
end
# now that we have run,
# we require a new run of any tasks _we depend on
@parent_status.keys.each do |k|
@parent_status[k]=false
end #each parent
if success
# let all dependents know we've run successfully
children=@schedule.select {|i| i.task.parent==@task.name }
children.each do |c|
c.parent_status[@task.name]=true
end # each
else # non-zero exit status
if @task.error_command
spawn do
ec=@task.hydrate_command(@task.error_command.not_nil!)
`#{Process.quote(ec)}`
end
sleep 0
end
if @task.global.mail
args=[] of String
args+=["-s", "task #{@task.name} exitted #{@last_status}"]
dn=log_dn(@last_start.as(Time))
fl=Dir.glob("#{dn}/*")
fl.each do |f|
args+=["--attach",f]
end # each file
args << @task.global.mail.not_nil!
body=IO::Memory.new
if @task.error_body
body << @task.error_body
body << "\n"
end
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
@errors.clear
ts=Time.local
start_channel.send(ts)
last_command=-1
rc=0
@task.commands.each_with_index do |c,idx|
last_command+=1
t=@task.hydrate_command(c)
begin
rc=run args: t, idx: idx, start_time: ts
rescue exc
rc=999
@errors << exc
end #rescue
break if rc!=0
end #each
events_channel.send({self, rc, last_command, Time.local})
end #def

def run(args : Array(String), idx : Int32, start_time : Time)
if @schedule.test
args=args.clone
args.unshift "echo"
end
dn=log_dn(start_time)
Dir.mkdir_p dn
File.write("#{dn}/#{idx}.cmdline", args.to_json)
error_fh=File.open("#{dn}/#{idx}.stderr", "wb")
output_fh=File.open("#{dn}/#{idx}.stdout", "wb")
begin
@sp = Process.new(
command: args[0],
args: args[1..-1],
error: error_fh,
output: output_fh,
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
# don't run a disabled task
if @task.disabled
return {WaitReason::Disabled, @task.name, 0.seconds}
end
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
if @task.parent && @parent_status[@task.parent.not_nil!] == false
return {WaitReason::Depend, @task.parent.not_nil!, 0.seconds}
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
@schedule = [] of TaskState
@test : Bool
@immediate : Bool
@filter : Set(String)
@crontab : String
@autosave : Time::Span = 0.seconds
property :test

delegate :select, to: @schedule

def initialize(@test, @immediate, @filter, @crontab)
end

def [](name : String)
@schedule.find! {|i| i.task.name==name }
end

def []?(name : String)
@schedule.find {|i| i.task.name==name }
end

def running
@schedule.select &.running?
end

def all_tasks_have_run_since?(start_time)
do_filter=@filter.size>0
ret=true
@schedule.each do |i|
if do_filter && ! @filter.includes?(i.task.name)
next
end # if filter
if ! i.has_run_successfully_since?(start_time)
ret=false
end # if task has not run
end # each task
ret
end # def

def clear_dependency_state
@schedule.each do |parent|
children=@schedule.select {|i| i.task.parent==parent.task.name }
children.each do |c|
# mark each child as needing it's parent to complete a fresh run before it can run
c.parent_status[parent.task.name]=false
end #each child
end #each parent
end # def

# you _must call clear_dependency_state
def add_tasks(tasks)
tasks.each do |t|
@schedule << TaskState.new(task: t, schedule: self)
end
end #def

# you _must call clear_dependency_state
# after loading state
def load_task_state?
path=@crontab+".state"
err=false
src=Path[path].expand(home: true)
if ! File.exists?(src)
return false
end
state=JSON.parse(File.read(src))
state.as_a.each do |ts|
task_state=self[ts["name"].as_s]?
if ! task_state
err=true
next
end
task_state=task_state.not_nil!
task_state.set_state ts
end # each
err==false
end # def

def save_state
path=@crontab+".state"
dest=Path[path].expand(home: true).to_s
File.write(
dest+".tmp",
@schedule.to_json)
File.rename(
dest+".tmp",
dest)
end

# handle this like a fresh start with saved state
# read crontab and saved state file,
# and apply saved state to all existing tasks
def load
ct=Crontab.new @crontab
ct.verify
Dir.cd ct.global.workdir
@autosave=ct.global.autosave
@schedule.clear
add_tasks ct.tasks
if ! @immediate
load_task_state?
end
clear_dependency_state
end

def autosave(run_state_chan, wait_time=600.seconds)
while 1
sleep wait_time
run_state_chan.send RunState::Save
end
end

def loop(run_state_channel : Channel(RunState)? = nil)
loop_start_time=Time.local
reasons=[] of Tuple(TaskState,Tuple(WaitReason, String, Time::Span))
chan=Channel(Time).new
events=Channel(Tuple(TaskState, Int32, Int32, Time)).new
drain_state=DrainState::None
run_state=RunState::Normal
shortest_timeout=1.hour
do_filter=@filter.size>0
load
if @autosave>0.seconds
spawn do
autosave run_state_channel, @autosave
end
sleep 0
end
while 1
#puts "while, drain #{drain_state}, run #{run_state}, running #{running.size}"
if drain_state.draining? && @schedule.none? {|i| i.running? }
drain_state=DrainState::Drained
end
if drain_state.drained?
if run_state.exit? || run_state.reload?
if ! @immediate
save_state
end # if not immediate
end # if exit or reload
if run_state.reload?
#read crontab
#update changed tasks
#add new tasks
load
run_state=RunState::Normal
drain_state=DrainState::None
next
end # if reloading or saving
if run_state.exit?
exit
end
end # if drained
if run_state.normal? && drain_state.none?
reasons.clear
@schedule.each do |i|
if do_filter && ! @filter.includes?(i.task.name)
next
end
reason=i.should_run?
if reason[0].none?
spawn do
i.run chan, events
end
sleep 0
started i, chan.receive
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
end # if normal and not draining
# wait on events from any task
#puts timeout_reasons
select
when t=run_state_channel.receive
if ! run_state.normal?
puts "requested run state #{t} but currently have run state #{run_state} drain state #{drain_state}"
next
end
if t.save?
# ignore draining here
# we want to save state in case of power outage, crash, etc
# we can afford to rerun currently running tasks
save_state
next
end
#wait for all running tasks to stop
#do not queue any further tasks
run_state=t
drain_state=DrainState::Draining
puts "run state #{run_state}"
next
when x=events.receive
stopped(x)
if @immediate && all_tasks_have_run_since?(loop_start_time)
break
end # if immediate mode
next
when timeout(shortest_timeout)
next
end #select
end #while
end #def

def started(task, start_time)
task.started start_time
end

def stopped(x)
x[0].stopped(status: x[1], last_command_index: x[2], stop_time: x[3])
end

end #class


def main
test=false
immediate=false
ct="~/.crdo.yml"
filter=Array(String).new.to_set
parser=OptionParser.new do |parser|
parser.on(
"-h",
"--help",
"show this help") do
puts parser
exit
end
parser.on(
"--file name",
"location of crdo file"
) do |name|
ct=name
end
parser.on(
"--now",
"run a single task without reading or writing task state"
) do
immediate=true
end
parser.on("--test",
"prefix all commands with echo") do
test=true
end
parser.unknown_args do |args|
filter=args.to_set
end
end
parser.parse
run_state_chan=Channel(RunState).new
Signal::HUP.trap do
run_state_chan.send RunState::Reload
end
Signal::INT.trap do
run_state_chan.send RunState::Exit
end
t=Schedule.new test: test, immediate: immediate, filter: filter, crontab: ct
puts "crdo running with pid #{Process.pid},#{immediate ? " immediate" : ""} #{test ? "test" : "normal"} mode"
t.loop run_state_chan
end

main
