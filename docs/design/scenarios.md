# Scenarios

Behaviour every layer must show, written to become the conformance suite. Each scenario lists the requirements it
covers from [problems](problems.md); every requirement is covered at least once. "Holder" is a process or remote
client holding a lease; "pause" means a holder stops running (a long GC pause, a frozen VM) without its process ending.
"Eventually" in an expectation means within one polling interval plus one lease duration.

## Leases

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| L1 | Twenty processes claim due records of one source concurrently. | Every record is claimed once; no claim waits on a record another claim is taking. | R2, R6 |
| L2 | Records due at different times, one process with capacity 1. | They are claimed in due order; a record due in the future is not claimed before its time. | E2 |
| L3 | A process creates a record due now and claims it by key while a dispatcher also races for it. | Exactly one claim succeeds; the other reports not claimable. | E3 |
| L4 | A holder pauses past its lease; another process claims the record; the first holder resumes and commits output. | The commit fails in `GuardAsync`; the second holder's work stands; epochs differ by one. | R3 |
| L5 | A holder pauses past its lease and nobody claims the record; the holder resumes and renews. | The renewal fails; an expired lease cannot be renewed. | R3 |
| L6 | A holder's guarded transaction is open while its background renewal fires and a claimer races for the record. | The renewal waits and then succeeds; the claimer skips the record; the transaction commits even if the lease's original expiry passed during it. | R3, E4 |
| L7 | A domain transaction locks a holder's record without changing it; the holder's renewal waits on that lock while the lease expires; then the lock is released. | The renewal fails: validity is evaluated with a clock reading taken after the lock was acquired. A claimer meanwhile skipped the locked record instead of waiting. | R3 |
| L8 | A holder's process is killed while holding a record. | The record is claimable again after the lease expires and the takeover backoff passes, with no other action; the claim is a takeover. | R5, R9, E9 |
| L9 | A record keeps killing its holders. | Takeover backoff grows; after `MaximumTakeovers` the next claim goes to the exhaustion handler, which ends the record with a domain reason. | R9, E9 |
| L10 | A process with capacity 2 and ten due records. | It holds at most two; the rest stay claimable for other processes. | R6 |
| L11 | A record is made due by a committed transaction; another by a rolled-back one. | A waiting dispatcher claims the first without waiting for its polling interval (providers with signals); nobody is woken for the second. | R1, R7 |
| L12 | Another process requests cancellation of a held record. | The holder's token is cancelled within one renewal interval with reason `cancel requested`. Only the holder's process renews early to observe it (providers with signals). | R8 |
| L13 | Cancellation is requested while the record waits for a retry; later the domain clears it. | A claimant before the clear sees the request at claim time; after the clear the record runs normally. | R8 |
| L14 | Graceful shutdown with work running past the grace period. | Records are released without a takeover or attempt and claimed elsewhere at once. | R11 |
| L15 | A holder writes progress with each renewal; it is killed; a new holder claims. | The new holder reads the last progress committed with a renewal. | E4 |
| L16 | A holder releases after a failure with a later due time, twice. | The record is not claimable before that time; `Failures` is 2 and `Takeovers` unchanged. | E5, E11 |
| L17 | The domain ends the lease and sets `DueAt` to null in its terminal transaction. | Nobody claims the record until the domain schedules it again. | E1, E6 |
| L18 | A handler returns without ending or releasing its record; another ends it in a transaction that rolls back. | Each record is released as failed with the source's backoff and `lease.unended` is reported. | E1 |
| L19 | Candidates are selected by a domain query; between selection and claim one becomes ineligible and another is claimed by someone else; one key is listed twice. | The claim in the caller's transaction claims only still-claimable keys, each once; the caller's recheck rejects the ineligible one; nothing is reserved for unclaimed keys. | E7 |
| L20 | A claim hook settles the previous attempt and inserts a new one; in a second run the hook throws; in a claim of due records the hook throws for the earliest one; in a fourth the hook runs longer than the lease. In a fifth, the hook of the earliest due record finds a row an application transaction holds and does not wait for it. | First: the hook sees reason `Takeover` with the previous owner and epoch, and its writes commit with the claim. Second: the claim rolls back and the record stays claimable. Third: that record counts a failure and is due after the failure backoff, and the next due record is claimed in the same call. Fourth: the claim is lost when its hold starts and the handler is not run. A due claim with five failing records and limit 2 makes exactly four attempts. Fifth: that record counts no failure and keeps its due time, the next due record is claimed in the same call, and a later call claims the first once the row is free. | E8 |
| L21 | A holder holding a capacity reservation disappears; nobody claims the record for an hour. Another expired record is locked by a domain transaction, and the expiry handler fails for a third. A fourth handler finds a row an application transaction holds and does not wait for it. | The expiry processor settles the expired epoch promptly and releases the reservation once. The locked and the failing record do not hold back the others. The failed settlement counts in `SettleFailures`, not `Failures`, leaves the record unclaimed and is retried after the settlement backoff, or on every processor pass with `Backoff.None`. The fourth counts no settlement failure and is settled by a later pass once the row is free. | E10 |
| L22 | Two expiry processors and a claimer race for the same expired epoch; a claimer without the expiry handler finds an unsettled expired lease; a claim of due records meets several expired leases, and the handler fails for one. | The epoch is settled exactly once; the claim proceeds after settlement. The claimer without the handler does not claim the record until a process with the handler settles it, and that settlement wakes it (providers with signals). The due claim settles and claims the others and leaves the failing one to its settlement backoff. | E10 |
| L23 | Spread limit 1 per partition, three processes, three long-held records of one partition. | Each process holds one; no process holds two while another holds none. | E12 |
| L24 | Two sources mapped to the same table with different fields; both have due work on one row. | Each source claims, renews and ends independently; neither changes the other's fields. | E13 |
| L25 | Process clocks are skewed by minutes in both directions. | Claims, expiry and backoff behave as with synchronized clocks. | R4 |
| L26 | Partition limit 1 with five due records in one partition and one in another, two processes. A claim is cancelled in its second pass. An application transaction claims in a partition and then writes another record of it while claims race for the partition. | At most one record of the busy partition runs at a time; the other partition's record runs immediately. The cancelled claim returns the claims its first pass committed. The racing due claim skips the busy partition and the racing claim by key waits holding no record lock, so the transaction does not deadlock. | R13 |
| L27 | Partition limit 1; a holder's guarded transaction stays open past the lease\'s previously committed expiry while another record of the partition is due. | The second record is not claimed while the first lease is outstanding; after the guard commits, exactly one lease of the partition is valid. | R13, R3 |
| L28 | Partition limit 1; a holder crashes. | Its lease keeps counting until the expiry processor settles it, then the next record of the partition is claimed; no limit is exceeded at any point. | R13, E10 |
| L29 | An application transaction claims in a partition and stays open; a due claim covers that partition and another; another transaction holds the owner's scope. | The due claim returns the other partition's record without waiting and claims the skipped one in a later call; with the owner's scope busy it returns at once without claims. | R13 |
| L30 | A due claim finds nothing shortly before a record becomes due, and shortly before an expired lease's takeover backoff ends; another due record is locked; a partition is at its limit. | The next due time after the claim's cutoff is zero once either record is claimable, so the dispatcher claims again instead of waiting for the polling interval; the locked and the limited record, which the claim considered, give no immediate next due time. | R7 |
| L31 | A record's key is as long as its column; another key adds a character to it. Schedules, cancellation requests and claims, by key and from candidates, use the longer key. | None matches the record or changes it; a candidate claim that also lists the record claims only the record. | E3, E7 |

## Remote holders

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| W-1 | A remote client claims through an API, renews with its token, and its process vanishes. | The record is reclaimed after expiry; a late result with the old token is refused. | W1, W2 |
| W-2 | Owner limit 2; a remote holder claims due records and then by key. Owner limit 1 on two sources. Owner limit 1 and an expired, unsettled lease that another owner takes over. | It never holds more than two, on either path. Each source counts its own leases. The expired lease counts for its owner, who may take it back but claim nothing else, until the other owner takes it over; then it counts for the new owner. | W3 |
| W-3 | A remote holder's completion commits but the reply is lost; it resends the same completion, then a different payload. | The resend returns the recorded outcome; the different payload is rejected. | W4 |

## Reconciliation

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| C-1 | Ten requests for one record before a run. | One run, which sees the latest revision. | C1 |
| C-2 | Two requests arrive while a run converges the record (follow-up policy). | Exactly one follow-up run after the current one. | C2 |
| C-3 | Requests every second with a five-second debounce and a one-minute maximum, with free capacity. | The record becomes due no later than a minute after the first outstanding request. | C3 |
| C-4 | A run reports waiting with a retry time. | The record is due at that time; failures are unchanged; a new request makes it due sooner. | C4 |
| C-5 | A run fails repeatedly. | Backoff grows and caps; the record is never terminal; an ordinary request does not move the due time before the backoff floor, and `ScheduleAsync` is refused by `LeaseOperations` and by the store; each failed result is logged with its code. | C5 |
| C-6 | The source is invalidated with a million converged records. | The invalidating transaction writes no records; every eligible record runs once; requested records keep being served meanwhile. | C6 |
| C-7 | The source is invalidated; a record's run reports waiting, another fails. | Neither becomes claimable again before its retry time; `AttemptedGeneration` is current and `ConvergedGeneration` is not. | C6 |
| C-8 | The source is invalidated while its records have `DueAt` null. | Each record runs once and converges under the new generation; its `DueAt` stays null. | C6 |
| C-9 | A converged record with a resync interval and no requests. | It is evaluated again after the interval. | C7 |
| C-10 | Two sources share a pool; one has a large backlog. | The other source's due records are claimed within one rotation. | C8 |
| C-11 | A request arrives during a run of a supersede source. | Another process claims at once without a takeover; the old run's token is cancelled with reason `Superseded` at its next renewal, which its process is signalled to make at once; its guards and results fail and its release changes nothing. | C9 |
| C-12 | A record is in backoff; a request with `ResetBackoff` arrives; another without it. | The first makes it due now and resets failures; the second leaves the backoff. | C10 |
| C-13 | A run completes with `CompleteAsync` in the domain transaction, which then rolls back. | Neither the domain status nor the work fields change; the record is still held and can complete again. | D3 |
| C-14 | An idle converged record (`DueAt` null) is claimed because of an invalidation; its holder crashes before returning a result. | After the lease is settled or taken over, the record is claimed again and converges; the invalidation is not lost. | C6, R5 |
| C-15 | On a supersede source, an invalidation is issued while a guarded transaction is open, and a guard of another record starts while it waits; another invalidation commits before a guard starts. | The first invalidation waits for the guarded transaction to end, whose output commits; the guard that started meanwhile waits for the invalidation and then fails; the second invalidation makes the later guard fail. | C9, C6 |
| C-16 | A superseding request arrives for a record whose previous holder has two takeovers. | The record is claimable at once without takeover backoff; the claim is not a takeover; the superseded lease stops counting against limits immediately. | C9, E9 |
| C-17 | On a supersede source with partition limit 1, a run holds a lease until 12:05; an invalidation commits at 12:01; another process claims at 12:01:10; later the old run renews. | The record is claimed at once: the obsolete lease is retired as a supersession with no takeover, no backoff and the partition limit respected, and the new lease is the only one counted. The old run's renewal fails and cancels its token; its guards and results change nothing. | C9, C6, R13 |
| C-18 | A record already due by time when an invalidation commits; batch claims with and without a claim hook. | Each record is claimed once, at epoch 1, without a takeover; the hook runs once per record. | C6, E1 |
| C-19 | A claim hook fails for a record due only because of an invalidation; the claimer claims again at once. | The hook runs once; the record has one failure and a backoff floor, and is not claimable before it. | C5, C6 |
| C-20 | A dispatcher with one slot; one record is always due again; another is due only because of an invalidation. | The invalidated record runs after at most two runs of the other. | C6 |
| C-21 | On a supersede source with owner limit 1, an owner holds a lease; an invalidation commits; the owner claims due records with limit 1. | It claims the record of its obsolete lease at once, without a takeover, and no other record. | C9, R13 |
| C-22 | On a supersede source, a transaction takes the fence and an invalidation waits for it; a guard queues behind the invalidation; the fenced transaction then writes the guarded record. | The write does not wait: the queued guard holds no record lock. After the fenced transaction commits, the invalidation commits and the guard fails. | C9, C6 |
| C-23 | A request for a key one character longer than the record's, whose key is as long as its column. | The request finds no record; the record's revision and due time are unchanged. | C1 |

## Jobs

The conformance suite runs scenarios stated in minutes with seconds: J-7 with a grace of 2 seconds, J-27 with a timeout
of 1 second and a handler that ignores it for 6 seconds. Recurring scenarios scale minutes and days to seconds: J-3,
J-4, J-14 and J-16 use intervals of 1 second, J-6 a cron expression every second with 3.5 seconds of downtime, J-12 a
3-second grid in place of a day, and J-18 a pause of 2.5 seconds. Scenarios that need an occurrence to be absent use
hourly schedules.

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| J-1 | Fire-and-forget request committed with a domain change; another rolled back. | One job runs; the rolled-back request never existed. | R1 |
| J-2 | One-shot jobs at a time, after a delay and now. | Each runs once, not before its time. | J1 |
| J-3 | `Every(interval, AfterCompletion)` with runs of varying length. | The gap between the end of one occurrence and the start of the next is at least the interval. | J1 |
| J-4 | `Every(interval, FixedRate, anchor)` with one slow occurrence, `Overlap.Wait`. | Starts stay on the grid; occurrences due during the slow one collapse into one that starts when it ends. | J1, J15 |
| J-5 | Daily cron at 02:30 in a time zone across both daylight-saving changes. | One occurrence per day: at 02:30 when it exists, at the first instant after the gap on the spring day, once on the autumn day. | J1 |
| J-6 | All processes are down across three cron occurrences, with `Missed.Skip` and `Missed.RunLatest`. | `Skip`: nothing runs until the next future occurrence. `RunLatest`: the latest missed occurrence runs once, then the schedule continues. | J2 |
| J-7 | A one-shot job with `Late.SkipAfter(2 min)` starts 3 minutes late; another exactly 2 minutes late; a request merges while a claim holds the late one; a follow-up becomes overdue during a long run. | The first ends skipped with no handler call and no attempt used and its merged request runs as a follow-up; the second runs; the overdue follow-up ends skipped without an attempt, through the outcome hook. | J2 |
| J-8 | Requests with the same subject while scheduled, then while running. | One outstanding job with the latest arguments; one follow-up after the running one. | J3, J4, D1 |
| J-9 | A request would merge into another partition's job. | It is refused. | J3, R13 |
| J-10 | A handler fails each attempt; another process crashes during one attempt. | The crash counts as an attempt; after the last attempt the job is failed with `attempts_exhausted`. | J5 |
| J-11 | A handler asks to retry after an hour while the backoff is ten seconds. | The retry is not before an hour. | J5 |
| J-12 | A daily recurring job with five attempts fails its first two attempts of an occurrence, then succeeds. | The three attempts belong to one occurrence; its outcome is completed; the next occurrence is the next day with attempts reset. | J6, J14 |
| J-13 | A recurring job's occurrence exhausts its attempts. | The occurrence ends failed and is recorded; the recurrence continues with the next occurrence. | J6, J14 |
| J-14 | An occurrence still retrying when the next one becomes due, with `Wait`, `Skip` and `Allow`. | `Wait`: the next starts after the retrying one ends. `Skip`: the next is recorded skipped. `Allow`: the next runs as its own job while the first keeps retrying. | J14, J15 |
| J-15 | `TriggerAsync` with no outstanding occurrence, with one waiting for a retry, with one running. | Starts a triggered occurrence without moving the recurrence; makes the retry due now with attempts kept; returns `AlreadyRunning` and queues nothing. | J14, J21 |
| J-16 | Two processes run a recurring job with `Overlap.Skip` while an occurrence runs for longer than the interval. | No two occurrences run at the same time anywhere; due ones are recorded skipped. | J15 |
| J-17 | A scheduled job is cancelled in a transaction; a running one is cancelled from another process; a held one whose holder then crashes is cancelled, also on its last attempt; a recurring job's current occurrence is cancelled. | The first never runs; the second's token is cancelled and it ends cancelled; the third ends cancelled, not failed, at its next claim without a handler or an attempt; the recurrence continues after the fourth. | J7 |
| J-18 | A definition is paused and resumed; one subject is paused while others run. | Nothing of the paused scope is claimed while paused; resuming computes the next due time under the missed and late policies. | J8 |
| J-19 | A fan-out of 50 000 subjects in one transaction, then a crash before commit. | No child exists; the rerun creates all of them exactly once. | J9 |
| J-20 | Three processes of two releases declare the same recurring job; the older release restarts last. | The stored recurrence and policies follow the newest revision; the older process applies them and is reported as outdated; equal revisions with different content fail at startup. | J10, R18 |
| J-21 | A recurring job declared only by a process that is no longer deployed. | Its record is kept and reported as undeclared until removed explicitly. | J10, R12 |
| J-22 | A stored job's argument version is below the newest definition's `MinimumArgsVersion`. | It ends failed with `args_version` without deserializing the arguments; the outcome hook runs. | J11, D4 |
| J-23 | A cron expression with no future occurrence is requested. | The request is refused. | J1 |
| J-24 | Finished jobs, chains, run history and stopped processes older than their windows. | They are deleted; outstanding and recent ones are kept. | J12 |
| J-25 | `IfAbsent` while a job is scheduled with backoff, while one runs, and while none is outstanding. | The first two leave the job's revision, arguments, schedule and attempts unchanged and create no follow-up; the third creates the job. | J13 |
| J-26 | High- and low-priority jobs are due in one pool with capacity 1. | The high-priority job is claimed first; a running low-priority job is not interrupted. | J17 |
| J-27 | An attempt times out; its handler honours cancellation; another handler ignores it for a minute. | The first ends timed out (or retries when the definition retries timeouts). The second keeps its lease and is not claimed twice; it ends once the handler returns, and `job.timeout_ignored` is reported. | J18 |
| J-28 | A handler reports progress; its lease is lost; the next attempt starts. | The late report from the old attempt is rejected; progress is cleared for the new attempt. | J19 |
| J-29 | A job returns a typed result; a caller waits for it with a timeout. | `WaitAsync` returns the outcome and result when the job ends; it returns a timeout when it does not end in time. | J20 |
| J-30 | `RunNowAsync`, `RerunAsync` and `RerunManyAsync` with duplicates, on terminal, scheduled and running jobs. | Running work is never duplicated; a rerun of a terminal job keeps its id with fresh attempts; refusals are reported per job. | J21 |
| J-31 | Definition limit 2 across three processes with capacity 2 each and ten due jobs; partition limit 1 with unpartitioned jobs and jobs of a named partition across two processes. | At most two run at a time across all processes; unpartitioned jobs run one at a time and alongside the named partition's. | J22 |
| J-32 | A process requests jobs of a definition it registers without a handler; a job and a recurrence of other definitions are written in transactions that commit after their subject's resume, so they keep a pause flag no stored pause backs. | It never runs them; they are reported as unserved while no live process runs the definition, and so is the work with the stale flag, before and after the flag is repaired; the flagged work of a paused subject is not. | R10, R12 |
| J-33 | A process performs a transactional operation on a definition it has not registered. | The operation is refused. | D4 |
| J-34 | `*/30 * * * *` and `30 1 * * *` in a time zone across both daylight-saving changes. | Occurrences match Cronos 0.13.0: in spring nothing is lost and occurrences in the gap run at the first valid time after it; in autumn the interval expression runs in both instances of the repeated hour, the daily expression only in the first. | J1 |
| J-35 | During a rolling deployment, a new release requests jobs with argument version 2 while old processes accept only version 1. | Old processes never claim the version-2 jobs and never fail them; the jobs run on a new process; while none is live they are reported as unserved for version 2, including a version-2 job a request racing a resume left flagged, once the old processes' maintenance repairs its flag. | R21, J11 |
| J-36 | A merged follow-up run, a triggered occurrence and a rerun of the same job. | Each has a new `ExecutionId`; retries within each keep theirs. | R17, J4, J21 |
| J-37 | A standalone request with the identity of a recurrence: while an occurrence is outstanding, while one runs, and with `IfAbsent`; `ScheduleAsync` while a standalone job of the identity is outstanding. | The request creates or merges a standalone job and never joins, merges into or waits for an occurrence; `IfAbsent` counts only standalone jobs; the schedule leaves the standalone job alone and plans its own occurrences. | J3, J13, J14 |
| J-38 | A subject pause that has not committed while a job of the subject is due; a request committed before a pause commits; a request that copied a pause and commits after the resume; a fan-out of 50 000 subjects, one of them paused. | No claim takes the job until the pause rolls back; the request's job is never claimed while paused and maintenance sets its flag; the racing request's job runs without further requests; the fan-out commits, with the paused subject's job flagged. | J8, J9 |
| J-39 | Application code calls lease operations on a job's source: claims, a release, an end with a lease parsed from a token, a schedule with a new execution, a cancellation request and its withdrawal. | Each is refused; the holder's renewal and guard still work, and the job keeps its holder, execution and attempt. | J5, D4 |
| J-40 | A dispatcher recurrence every few seconds requests standalone jobs per subject, while manual requests of the same subjects arrive before and during their runs; `TriggerAsync` of an `Overlap.Wait` recurrence during a run. | Each subject's scheduled and manual requests merge or become one follow-up, and no two runs of a subject overlap; the trigger returns `Waiting` and one run follows the running occurrence. | J3, J4, J15 |
| J-41 | A request without a transaction for a subject with an outstanding job, then one for another subject while a worker idles with a long polling interval. | The first merges into the outstanding job like a transactional request; the second is visible without any caller commit and its commit wakes the worker, which runs it before its next poll. | J3, R1 |

## Calendars

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| CAL-1 | A daily cron at 07:00 within a business-day calendar, across a weekend and a public holiday from an application date provider. | Occurrences on blocked days are recorded skipped; others run at 07:00 local time. | J16 |
| CAL-2 | One-shot jobs at a blocked instant with `Reject`, `Skip` and `MoveForward`. | The request is refused; the job ends skipped at its due time; the job runs at the next allowed instant. | J16 |
| CAL-3 | A window crossing midnight; a calendar composed with `Except`. | Allowed instants match the composed rule; a cycle between calendars fails at startup. | J16 |
| CAL-4 | A job due at 17:50 inside an 08:00–18:00 window cannot start before 18:00 for lack of capacity, with `Closed.Wait` and `Closed.Skip`. | `Wait`: it starts at 08:00 on the next allowed day. `Skip`: it ends skipped with `window_closed`. No claim happens outside the window. | J16 |
| CAL-5 | A retry's backoff ends outside the window; an operator triggers the job outside the window with and without `IgnoreCalendar`. | The retry starts at the next allowed instant without an extra attempt; the trigger waits for the window unless it ignores the calendar. | J16, J21 |
| CAL-6 | A calendar uses a published holiday date set; a newer revision adds a holiday; another process plans an occurrence beyond the covered range; the set's coverage ends within the margin of a job's instant or a recurrence's next instant. | Every process plans the same occurrences; the new revision re-plans the affected occurrence; the uncovered occurrence waits as `calendar_unavailable` and is planned once a covering revision is published. The coverage report shows the set with work waiting, then covered; a set whose coverage ends within the margin is running out until a revision covers further. A monitoring process opens `DateSetRunningOut`, then `DateSetWorkWaiting` while the first stays open, and resolves each when its condition clears. | J16, M2 |
| CAL-7 | A calendar used by an outstanding job, a recurrence and a composing calendar is removed; then only ended jobs use it; a process registering another calendar stops. | Removal is refused, naming the blockers, then succeeds; a rerun of an ended job is refused as `CalendarRemoved`; the stopped process's calendar is reported as registered by no live process. | J16, R12 |

## Chains

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| K-1 | A three-step chain; the second step fails; the third runs on success, a fourth on failure. | Steps run in order; the third is skipped; the fourth runs; the chain is created atomically or not at all. | K1 |
| K-2 | A group of three branches, one failing, with all four continuations. | Failure continuations start only after every branch settled; `AfterAll` is skipped; `AfterAnyFailed`, `AfterAnyFailedOrCancelled` and `AfterAllSettled` run. | K2 |
| K-3 | A step returns a typed result; its child reads it; a group continuation reads its branches' results, one branch continuing into a nested group. | The values round-trip through the result codecs; a step followed by a nested group does not end its branch. | K3 |
| K-4 | A step adds a dynamic group of 10 000 children with an after-all-settled continuation; one child takes an hour. | The continuation runs exactly once, after the slowest child settles. | K4 |
| K-5 | A recurring chain with `Overlap.Skip` whose previous chain still has a running step. | The new occurrence is recorded skipped; no chain is created. | K5, J15 |
| K-6 | A settled chain with a failed middle step is retried from it, twice concurrently. | One retry wins; completed ancestors do not run again; the step and its dependants run under a new generation; a late write from the old generation is rejected. | K6 |
| K-7 | A chain root skipped by a calendar and another by `Late.SkipAfter`. | Every step settles skipped without a handler call. | K7 |
| K-8 | Deleting a chain with a running step, by default and with cascade; retention of a settled chain. | Default refuses; cascade cancels the step and deletes the chain once it settled; retention deletes the chain as a unit. | K8 |
| K-9 | An empty dynamic group; a group whose branches are all skipped; a group with one succeeded and two skipped branches; a branch whose first step failed and whose second was skipped. | Empty: settles at once as succeeded. All skipped: skipped, so only `AfterAllSettled` runs. One succeeded: succeeded. The branch counts as failed. | K2, K4 |
| K-10 | A step waits for its parent: its status is read, it is run now, and its subject is cancelled; after the chain settled, a step is rerun. | The status is `WaitingForParent` without a due time; run now and rerun are refused as `ChainStep`; the cancellation ends it cancelled and its `OnCancelled` child runs while its `OnSuccess` child is skipped. | K1 |
| K-11 | A settled chain whose nested group failed is retried from that group. | The nested group's branches and continuation and the enclosing group's continuations run again under a new generation; the enclosing group's other branches do not; the failure continuation is skipped this time. | K6 |
| K-12 | A settled chain is retried from a step inside a group that added a dynamic group. | The step adds its group again and each child runs once; the previous dynamic group is gone; the enclosing group's continuation runs. | K4, K6 |
| K-13 | A chain is cancelled while a step's terminal transaction, in its outcome hook, holds the step and has not decided its children yet. | The cancellation waits for the step and then cancels its children; neither transaction fails with a deadlock. | K1, K8 |
| K-14 | A recurring chain's occurrence waits for its chain, whose root is held for a second; it is run now. | Run now reports it running; once the chain settled it ends completed with one run history entry spanning the chain. | K5, D7 |
| K-15 | A subject's cancellation holds a waiting step in its outcome hook, before deciding the step's 16 children; the step's chain is cancelled meanwhile. | The chain's cancellation waits for the subject's; both commit without a deadlock and every outstanding step ends cancelled. | K1, K8 |
| K-16 | A recurring chain's code-defined recurrence is removed while its occurrence's chain runs, its root held and a step waiting. | The occurrence stays running; the root and the waiting step end cancelled without the step running; the occurrence then ends cancelled with `chain_cancelled` and one run history entry. | K5, J17 |
| K-17 | A step reads its parent's result of version 2 with a codec that reads up to version 1, as an older release would; the outcome is read with the same codec. | Each attempt fails with `result_version` before the handler goes on, the step ends `attempts_exhausted`; the outcome read throws `JobResultVersionException`. | K3, R21 |

## Jobs on domain records

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| D-1 | The subject is deleted while its job is scheduled and while it runs. | The scheduled job is cancelled in the deleting transaction; the running job's later output is rejected inside its own transaction. | D2 |
| D-2 | The handler completes with `CompleteAsync` in the transaction that writes the subject's status, and the process dies right after commit. | Status and job agree; no second run happens for that request. | D3 |
| D-3 | Attempts are exhausted because every holder lost its lease. | The outcome hook records the failure on the subject in the transaction that ends the job. | D4 |
| D-4 | Terminal transitions by `CompleteAsync`, by a returned result, by an operator rerun's cancellation, by a request-only process cancelling, and by a skipped occurrence. | The hook runs once in each transition's transaction, through the same path. | D4 |
| D-5 | The outcome hook throws. | The transition rolls back and is retried; the hook's committed effects appear exactly once. | D4 |
| D-6 | A list of 500 subjects reads its job status; a recurring subject under `Overlap.Allow` has several outstanding occurrences. | One batched read returns state, next due time, attempt, progress and last outcome for each; the recurring subject shows the occurrence holding its identity. | D5 |
| D-7 | A user changes a subject's sync interval while its job runs. | The new recurrence applies from the next occurrence after the current one. | D6 |
| D-8 | Run history is enabled; a job runs three attempts. Attempts also end by timeout, retried timeout, failure and exhaustion; by a graceful release, a failed release and a stale one; by a lease expiry that is settled without a claim; occurrences of a recurring job run; a late claim ends a job without running it. | Three entries with epochs, outcomes and reason codes; each other attempt has one entry with its outcome and reason, occurrences with their occurrence; a graceful release is `Released` and returns its attempt, a failed one is `LeaseLost` ended at the release, a stale one records nothing, and a settled expiry is `LeaseLost` ended at the expiry; the late claim records no entry. Retention removes them independently of the job. | D7 |
| D-9 | A handler calls an external API with `ExecutionId` as idempotency key, then loses its lease before its guarded commit; later an operator reruns the finished job. | The next attempt has the same `ExecutionId` and a new `AttemptId`, so the repeated call is deduplicated; the rerun has a new `ExecutionId` and is not suppressed. | R17 |

## Monitoring

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| M-1 | A recurring chain expected to complete by 08:00 on business days finishes at 08:12; another day it finishes on time; on a holiday it does not run; every worker of the definition is stopped before a deadline. | An incident opens at 08:00 and resolves at 08:12, and the next day needs a completion after 08:12; nothing opens on time or on the holiday; with the workers stopped, a monitoring process opens the incident at the deadline. | M1, M2 |
| M-2 | An attempt runs longer than its expectation; a job fails four times in a row with a limit of three; jobs of another subject keep succeeding. | One incident for each, naming the job and the subject; each resolves after the subject's next success; the other subject opens nothing. | M1, M2 |
| M-3 | The incident handler's process crashes during delivery; an incident resolves before its opening was delivered. | The handler is called again; the incident is opened once; the second incident is delivered opened, then resolved. | M2 |
| M-4 | Telemetry from claims, runs, a takeover, a skipped occurrence and an incident; a job requested inside a trace. | Spans, counters and histograms carry definition keys, outcomes and reason codes; lateness is recorded; the run span links to the request's trace. | M3 |
| M-5 | Definitions declare expectations and no live process monitors. | `GetUnmonitoredAsync` lists them; once a monitoring process starts, it lists none. | M2, R12 |

## Hosting

| ID | Scenario | Expected | Covers |
|---|---|---|---|
| H-1 | Two roles register different sources against one store. | Each claims only its own sources; neither needs the other running. | R10 |
| H-2 | Two applications share one database with the same definition keys, calendar names (different rules) and subjects; one monitors, delivers incidents and runs retention. | Both start; each claims and reads only its own jobs; unserved and unmonitored reports, the process list and incidents cover one application; retention leaves the other's ended jobs. | R19 |
| H-3 | Telemetry from a failing job whose error message contains user data, and from a unique violation whose detail contains a key. | Spans and framework logs carry the definition key, reason code and exception type, not the message, detail or arguments; with `ExceptionDetails()` the logs carry the exception. | R14 |
| H-4 | The packages, except `Corfedo.Work.EntityFrameworkCore`, are published as a trimmed Native AOT application. | No trimming or AOT warnings; registrations, codecs and handlers work. | R15 |
| H-5 | The conformance suite runs against every provider. | All scenarios pass, except those a provider documents as needing a capability it lacks (for example wake signals). | R16 |
| H-6 | An application's handler and chain tests run on the in-memory provider with a controlled clock. | Every single-process scenario behaves as on PostgreSQL; time advances only when the test advances it. | R20 |
| H-7 | A host starts with `StartMode.Manual` and is started after initialization; a host is built and never started. | No claims, process record or planned occurrence before `IWorkHost.StartAsync`; normal dispatch after it; `IWorkHost.StartAsync` is refused before the host started and in automatic mode. The unstarted host claims and registers nothing. | R10 |
