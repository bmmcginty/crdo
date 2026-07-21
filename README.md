# CRDO

A small cron-like scheduler written in Crystal.
Inspired by [this HN comment](https://news.ycombinator.com/item?id=37879760).

CRDO runs named tasks from a YAML file, keeps lightweight run state between
process restarts, writes per-command logs, and can reload its config while tasks
are still running.

## AI

Codex has been used heavily in the refactoring of this tool. Commits using AI
are denoted with (AI tool name) in the author field.

## Usage

Create `~/.crdo.yml`, or pass another file:

```sh
shards build
./bin/crdo
```

Run one or more named tasks immediately without reading or writing scheduler
state:

```sh
./bin/crdo --now backup
./bin/crdo --now backup verify-backup
```

Preview command execution by prefixing each command with `echo`:

```sh
./bin/crdo --test --file ./sample.crdo.yml
```

Run specs with:

```sh
crystal spec
```

## Controls

Send signals to a running CRDO process:

```sh
kill -HUP  <pid>  # reload config
kill -USR1 <pid>  # print full schedule report
kill -USR2 <pid>  # print running task report
kill -INT  <pid>  # save state and exit after running tasks finish
```

On reload, unchanged running tasks are kept. Changed running tasks are marked as
retiring and their replacements are deferred until the old run exits. Deleted
running tasks are also allowed to finish before being removed.

`global.print_report: false` suppresses automatic task start/stop lines, but
USR1 always prints the full schedule report.

## Config

The root YAML object must contain `global` and one or more task entries.

```yaml
global:
  workdir: .
  include:
    - sample.include.crdo.yml
  mail: user@example.com
  autosave: 600
  ignore_overtime: false
  print_report: true
  test: false
  error: false
```

Global keys:

* `workdir`: required base directory for relative command paths and `cron_logs`.
* `include`: optional string or list of YAML files to merge into the root config.
* `mail`: optional address for failure and overtime mail.
* `autosave`: optional state save interval in seconds. Defaults to 600.
* `ignore_overtime`: skip overtime warnings for tasks that run past their interval.
* `print_report`: print automatic start/stop lines when tasks change state.
* `test`: prepend commands with `echo`.
* `error`: in test mode, treat completed commands as failed for dependency testing.

Includes are resolved relative to the config file. Include files may define tasks
only; they cannot define another `global` section.

## Tasks

Each task is keyed by name:

```yaml
backup:
  every: 1h
  timeout: 30m
  group: $exclusive
  vars:
    target: /srv/backup
  error_body: "Check disk space and rerun with crdo --now backup."
  error_command: /usr/bin/tmux new-window -d -n backup-error /bin/sh
  commands:
    - /usr/bin/rsync -a /home/ $target/
    - /usr/bin/true
```

Task keys:

* `commands`: required array of commands. Commands are executed directly, not through a shell, unless you explicitly use `/bin/sh -c`.
* `every`: interval schedule. Supports `s`, `m`, `h`, and `d`.
* `timeout`: optional maximum wall-clock runtime for the whole task. Supports `s`, `m`, `h`, and `d`.
* `when`: wall-clock schedule such as `13:00`, `mon 08:30`, `jan 1 00:00`, or comma lists like `mon,wed,fri 02:00`.
* `when_policy`: optional policy for missed/repeated wall-clock slots.
* `use_stop_time`: with `every`, measure the next run from the prior stop time instead of start time.
* `parent`: require another task to complete successfully before this task can run once.
* `group`: serialize tasks with the same group. `$exclusive` prevents any other task from running at the same time.
* `disabled`: keep the task loaded but never run it.
* `vars`: task-local string replacements for `$name` style variables.
* `error_body`: extra body text for failure mail.
* `error_command`: command to launch after a task failure.

A task must specify exactly one of `every` or `when`.

## Wall-Clock Policy

`when_policy` controls how `when` schedules behave across clock jumps.

```yaml
daily:
  when: 01:00
  when_policy:
    forward: after
    backward: once
  commands:
    - /bin/true
```

Forward policies:

* `skip`: missed slots are skipped.
* `after`: a missed slot runs after the scheduler notices the forward jump.

Backward policies:

* `once`: repeated local-time slots run once.
* `repeat`: repeated local-time slots may run again.

`when_policy: true` means `forward: after` and `backward: once`.

## Logs And State

CRDO writes command logs under:

```text
<workdir>/cron_logs/<task>/<yyyy-mm-dd>/<hh-mm-ss>/
```

Each command writes:

* `<jobnum>.cmdline`: JSON command argv.
* `<jobnum>.stdout`: captured stdout.
* `<jobnum>.stderr`: captured stderr.
* `<jobnum>.rc`: process exit code.

If failure mail cannot be delivered, CRDO writes `mailfail` in the task log
directory and includes recent mail failures in the USR1 report.

Scheduler state is saved next to the config as `<config>.state`. `--now` skips
state restore and save.

## How It Works

CRDO has one runtime loop:

1. Load config and restore state.
2. Start every task whose current state says it can run.
3. Wait for the next task stop, signal event, autosave, or schedule timeout.
4. Apply the event, update task state, print reports, reload, save, or exit.
5. Repeat until shutdown is requested and all running tasks have stopped.

The scheduler intentionally does not persist dependency readiness across
restarts. After a restart, parent tasks must run successfully again before child
tasks become eligible.

## Possible Future Work

* Retry-after-error delay.
* Randomized start delay.
* Configurable shutdown grace period before killing running tasks.
* Multiple parents or groups per task.
* Run tasks as another user.
