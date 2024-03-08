# CRDO

A cron tool written in crystal.
Inspired by [this HN comment](https://news.ycombinator.com/item?id=37879760).

## Usage

Make a .crdo.yml file in your home directory. See sample.crdo.yml for an example.

```
crystal build crdo.cr
./crdo
```

## Todo

* reparse and reload config on sighup
* import and export (so we don't always run everything on startup)
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

* job dependencies
* job anti-dependencies

## How It Works

Crdo runs in a loop.
The loop waits for the sooner of:
* a task finishing
* a timeout occuring because of a task needing to start
Crdo then checks through all tasks and starts any tasks that are startable.
