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

* time zones (Time.local+x.days and Time.local.shift(days: x) give different results, the ladder of these ending up in a never-ending loop)
* delayed randomized start
* time window (instead of the current `when` key
* retry period
* job timeout
* depends and group should actually be lists

## Done

* job dependencies
* job anti-dependencies

## How It Works

Crdo runs in a loop.
The loop waits for the sooner of:
* a task finishing
* a timeout occuring because of a task needing to start
