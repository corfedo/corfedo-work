# Calendars

A calendar is a named, reusable rule for when work may start, in local time. It keeps business hours, holidays and
maintenance windows out of handlers.

Requirement: J16 in [problems](problems.md).

## Defining a calendar

```csharp
work.AddCalendar(WorkCalendar.Create("business-days", revision: 2)
    .InTimeZone("Europe/Warsaw")
    .AllowWeekdays(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday)
    .AllowBetween(new TimeOnly(8, 0), new TimeOnly(18, 0))
    .ExcludeAnnual(month: 12, day: 25)
    .ExcludeDate(new DateOnly(2027, 1, 2))
    .ExcludeDates("public-holidays")               // application-provided dates
    .Except("maintenance"));                       // composition: minus another calendar
```

- A window's start is inclusive and its end exclusive. An end before the start crosses midnight: the window belongs
  to the day it starts on. Equal start and end allow a full day from the start. Without windows, allowed days are
  available all day.
- Local times map to instants with `TimeZoneInfo`: a local time in a spring gap maps to the end of the gap, and an
  ambiguous local time in an autumn overlap to its first instance.
- Calendars compose: `Except` subtracts another calendar, `Also` adds one. At startup the newest revision of every
  calendar, registered or stored, must compose without unknown names or cycles.
- Calendars are evaluated in .NET with the store's clock, against the calendars and date sets the store holds as the
  planning transaction reads them, so every process decides alike. Planning looks at most one year ahead.

### Storage and removal

Calendars are stored by revision like definitions (R18): a process stores its calendar unless a newer revision is
stored, and an equal revision with another rule is refused at startup. Processes recorded in the
[process registry](hosting.md#processes) also record the calendars they register.

`CalendarOperations.RemoveAsync(name)` deletes a stored calendar only while no live work uses it: outstanding jobs,
recurrences or another stored calendar composing with it. The refusal names the blockers, with the outstanding jobs
counted per definition. Ended jobs do not block: they keep the calendar's name without a reference. A rerun of such a
job is refused as `RerunOutcome.CalendarRemoved` once the calendar is gone; a process that still registers the
calendar stores it again at its next start. `CalendarOperations.GetUnregisteredCalendarsAsync()` lists stored
calendars that no live process registers, the candidates for removal.

## Application-provided dates

Dates the application owns, such as public holidays per country, come from a **date set**: a named list of dates
stored in the framework's tables, with a data revision.

```csharp
await calendars.PublishDatesAsync(tx, "public-holidays", revision: 7, dates, coversThrough: new DateOnly(2027, 12, 31), cancellationToken);
```

- **Same answer everywhere.** Processes read the stored date set, never a local service, so every process plans the
  same occurrences.
- **Revisions.** Publishing a newer revision replaces the set in the caller's transaction and re-plans the outstanding
  jobs that are not running of every calendar using the set: planned ones from their due time, and ones waiting for
  dates from their instant. An older revision never replaces a newer one; an equal revision with other dates or
  coverage is refused with `CalendarConflictException`.
- **Bounded lookahead.** A set states the last date it covers. Planning never looks further ahead than one year.
- **Unavailable data.** A job whose instant needs a date beyond the covered range is not planned: it waits for the
  dates, is reported, and is planned as soon as a covering revision is published. The framework never guesses that an
  uncovered date is allowed or excluded.

Applications publish date sets from wherever they keep them (a recurring job is a natural fit).

### Coverage report

`CalendarOperations.GetDateSetCoverageAsync()` reports date sets ahead of need. Each entry is a published date set, or
one a stored calendar uses before it is published, with its revision, the last date it covers, the calendars using it
directly or through composition, and a state:

| State | Meaning |
|---|---|
| `WorkWaiting` | Outstanding jobs of a calendar using the set wait for dates. The set is unpublished or, among the sets that calendar uses, covers the fewest dates. |
| `RunningOut` | The set is unpublished, or its coverage ends within the margin of now, of an instant of an outstanding job still to be planned, or of a recurrence's next instant within the lookahead. |
| `Covered` | Otherwise. |

The margin is 30 days by default, set with `work.CalendarCoverage(runningOutMargin)`, and compared by UTC days. The
report makes existence checks only and changes no record. Every process that registers a calendar reads it at the
polling interval and logs a set's state when it changes: `calendar.date_set_running_out`, `calendar.work_waiting` and,
when it is covered again, `calendar.date_set_covered`. A process logs a set that is not covered once at startup. Monitoring
processes also open [incidents](monitoring.md#date-set-coverage) for sets running out and sets work waits for.

## Applying a calendar

```csharp
Schedule.Cron("0 7 * * *", "Europe/Warsaw").Within("business-days")
Schedule.At(time).Within("business-days", Blocked.MoveForward, Closed.Wait)
```

A calendar is checked twice: when the due time is planned, and when the work actually starts.

**Planning.** A job with a calendar is stored with its requested instant and without a due time. When the instant
comes, the definition's [planner](hosting.md#job-planners) plans it: an allowed instant becomes the due time, and a
blocked one follows the schedule:

| Schedule | Blocked planned instant |
|---|---|
| Cron, interval | The occurrence is recorded as skipped with reason `calendar_blocked`, and the schedule advances. |
| One-shot (`At`, `After`, `Now`) | `Blocked.Reject` refuses the request; `Blocked.Skip` ends it as skipped at its due time; `Blocked.MoveForward` (default) plans it at the next allowed instant. |
| Chain | The root's rule applies; a skipped root skips the chain. |

`Blocked.Reject` is checked when the request is made, against the stored calendars and date sets: a blocked instant
throws `CalendarBlockedException`, and one a date set does not cover yet throws it with `Unavailable`. An instant
accepted then and found blocked when it is planned, after a newer date-set revision, ends skipped. A job with no
allowed instant within the lookahead ends skipped with `calendar_blocked`.

Planning at the instant keeps one planning path for every schedule, and a date-set revision published before the
instant applies without re-planning.

**Starting.** Planning stores the end of the allowed window with the due time, and claims require the claim time to be
before it. A takeover of a lost lease is not held to the window: the execution started inside it. Work that was due
inside a window but could not start before the window closed (no capacity, limits, an outage, a planner that came late)
follows the schedule's `Closed` policy; when the calendar allows the current instant, for example because a newer
date set extended the window, the work keeps its due time with the new window end instead.

| `Closed` | Effect |
|---|---|
| `Closed.Wait` (default) | Planned again at the next allowed instant. For recurring work, `Overlap` and `Missed` then decide between it and later occurrences. |
| `Closed.Skip` | Ended as skipped with reason `window_closed`. |

The planner wakes at the earliest calendar instant or window end of its definition.

**Merging.** A request that merges into an outstanding job before it starts replaces the job's calendar and policies
with its own, as it replaces the arguments, and merges the instant as its schedule merges due times. A request that
becomes the pending follow-up runs under the job's calendar, and a follow-up of a job with a calendar is planned from
its pending instant. A one-shot job recorded to be skipped by its calendar takes requests as follow-ups.

**Retries** are planned like any due time: a retry whose backoff ends outside a window moves to the next allowed
instant and does not use an extra attempt.

**Operator actions** (`RunNowAsync`, `TriggerAsync`, `RerunAsync`, `RerunManyAsync`) respect the calendar by default
and plan the work at the next allowed instant. With `JobActionOptions.IgnoreCalendar` the work is due now and may start
outside the calendar's windows; a date-set revision published before it starts does not plan it again. Its retries
respect the calendar.

`Late` is measured from the planned instant, so a job moved forward by a calendar is late only relative to the moved
instant. Calendars restrict when work may **start**; a running attempt is not interrupted when its window closes.
`JobStatus` shows a job waiting for its calendar instant as `Scheduled` with that instant as `DueAt`.
