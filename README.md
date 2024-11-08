# CRDO

A cron tool written in crystal.
Inspired by [this HN comment](https://news.ycombinator.com/item?id=37879760).

## Usage

Make a .crdo.yml file in your home directory. See sample.crdo.yml for an example.

```
crystal build crdo.cr
./crdo
```

## Controls

You can send signals to the running crdo instance:
- HUP reloads the config (and adds/removes tasks as needed)
- USR1 prints a state report for all tasks
- USR2 lists running tasks and the amount of time each has been running
Before the config is reloaded, all running jobs are allowed to complete, and new jobs are not queued.

## Notes

I have explicitly chosen not to track the state of dependent tasks across restarts.
After a restart, all parent tasks must run once before their child tasks are eligible to run again.
If this tool handled reloading of dependent statuses,
the load/save functionality would immediately gain complexity,
and saved state would cause a causal link between (possibly) changed dependent values in the yml file.

## Todo

* on hup or ^c, allow killing running tasks after a configurable timeout
* Ensure that a task does not run more than once in an assigned time period (other than retry-after-error)
* specs
* time zones (Time.local+x.days and Time.local.shift(days: x) give different results, the ladder of these ending up in a never-ending loop)
* delayed randomized start
* time window (instead of the current `when` key)
* retry period (retry-after-error)
* job timeout

## Maybe

* parent and group should actually be lists
* use sudo to run as a different user

## Done

* use --now to avoid writing task state for one-off runs
* reparse and reload config on sighup
* import and export (so we don't always run everything on startup)
* job dependencies
* job anti-dependencies

## How It Works

Crdo runs in a loop.
The loop waits for the sooner of:
* a task finishing
* a timeout occuring because of a task needing to start
Crdo then checks through all tasks and starts any tasks that are startable.
