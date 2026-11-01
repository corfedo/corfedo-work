using System.Globalization;
using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;

namespace Corfedo.Work.InMemory.Leases;

/// <summary>
/// The conditions of one lease source over its records, evaluated with a clock reading, the source's generation and the
/// stored subject pauses, and its process-local claim turn. Job sources share the application's job table: every operation is scoped to the source's definition.
/// </summary>
internal sealed class LeaseRules<TKey>
    where TKey : notnull
{
    private readonly ApplicationState application;
    private readonly HashSet<int> accepted;

    public LeaseRules(LeaseSource<TKey> source, InMemoryWorkDatabase database, ApplicationState application)
    {
        Source = source;
        Database = database;
        this.application = application;
        Fields = source.Fields;
        Reconcile = source.Reconcile?.Fields;
        Supersedes = source.Reconcile is { Supersedes: true };
        Policy = source.Policy;
        Job = source.Job;
        accepted = Job is { } job ? [.. job.AcceptedArgs.Where(v => v >= job.Policy.MinimumArgsVersion)] : [];
    }

    public LeaseSource<TKey> Source { get; }

    public InMemoryWorkDatabase Database { get; }

    /// <summary>The definition, revision, accepted argument versions and stored policy on job sources; null otherwise.</summary>
    public JobSourceSettings? Job { get; }

    public LeaseFields Fields { get; }

    /// <summary>The reconciliation fields on reconciliation sources; null otherwise.</summary>
    public ReconcileFields? Reconcile { get; }

    public bool Supersedes { get; }

    public LeasePolicy Policy { get; }

    /// <summary>
    /// Process-local claim state: whether the next due claim starts with records due only because of an invalidation.
    /// Claims of the source alternate it.
    /// </summary>
    public bool GenerationTurn { get; set; }

    /// <summary>The source's records; looked up on use, since a source may be defined before its table is created.</summary>
    public Table<RowKey, Record> Rows => Database.Table(Source.Store).Rows;

    /// <summary>The record belongs to the source: on job sources, to its definition.</summary>
    public bool InScope(Record r) => Job is null || r.Text(JobColumns.Definition) == Job.Definition;

    /// <summary>The source's record with the key as the transaction sees it, or null.</summary>
    public Record? Find(InMemoryTransaction transaction, RowKey key) => transaction.Get(Rows, key) is { } r && InScope(r) ? r : null;

    /// <summary>The source's records as the transaction sees them, in no particular order.</summary>
    public IEnumerable<(RowKey Key, Record Row)> Scan(InMemoryTransaction transaction) => transaction.Scan(Rows).Where(r => InScope(r.Row));

    /// <summary>The source's committed records; call under the database's lock.</summary>
    public IEnumerable<Record> Committed() => Rows.Committed.Values.Where(InScope);

    /// <summary>Whether a stored pause holds a subject, as the transaction sees the stored pauses.</summary>
    public Func<PauseKey, bool> Pauses(InMemoryTransaction transaction) => key => transaction.Get(application.Pauses, key) is not null;

    /// <summary>Whether a stored pause holds a subject, by the committed pauses; call under the database's lock.</summary>
    public Func<PauseKey, bool> CommittedPauses() => application.Pauses.Committed.ContainsKey;

    /// <summary>
    /// On job sources, the scope plus an argument version this process accepts and not below the stored minimum, and a
    /// subject without a copied or stored pause; always true otherwise.
    /// </summary>
    public bool Eligible(Record r, Func<PauseKey, bool> paused) =>
        Job is null
        || (InScope(r)
            && accepted.Contains(r.Int32(JobColumns.ArgsVersion))
            && !r.Bool(JobColumns.Paused)
            && !(r.PauseKey() is { } subject && paused(subject)));

    /// <summary>On job sources, the claim finds the job late before its execution started; uses no attempt.</summary>
    public bool Late(Record r, DateTimeOffset now) =>
        Job?.Policy.Late.Grace is { } grace && !r.Bool(JobColumns.Started) && Due(r) is { } due && now > due + grace;

    /// <summary>On job sources, the claim finds no attempt left after a lost lease; uses no attempt.</summary>
    public bool Exhausted(Record r) => Job is { } job && r.Bool(JobColumns.Started) && r.Int32(JobColumns.Attempts) >= job.Policy.Attempts;

    public RowKey RowKey(TKey key)
    {
        var writer = new KeyWriter();
        Source.Key.Write(key, writer);
        return new RowKey(writer.Values);
    }

    public TKey Key(RowKey key) => Source.Key.Read(new KeyReader(key.Values));

    public string Format(RowKey key) => Source.Key.Format(Key(key));

    /// <summary>The source's current generation; 0 before its first invalidation and on plain lease sources.</summary>
    public long Generation(InMemoryTransaction transaction, ApplicationState application) =>
        Reconcile is null ? 0 : transaction.Get(application.Generations, Source.Name)?.Value ?? 0;

    public DateTimeOffset? Due(Record r) => r.Time(Fields.DueAt);

    public DateTimeOffset? Expires(Record r) => r.Time(Fields.LeaseExpiresAt);

    public string? Partition(Record r) => Source.Partition is { } field ? Convert.ToString(r[field], CultureInfo.InvariantCulture) : null;

    public TimeSpan TakeoverBackoff(Record r) => Policy.TakeoverBackoff.Delay(Math.Max(r.Int32(Fields.Takeovers), 0));

    public TimeSpan FailureBackoff(Record r) => Policy.FailureBackoff.Delay(Math.Max(r.Int32(Fields.Failures), 0));

    public TimeSpan SettlementBackoff(Record r) => Policy.SettlementBackoff.Delay(Math.Max(r.Int32(Fields.SettleFailures), 0));

    /// <summary>The record's latest attempt started from an older generation than the current one.</summary>
    public bool Stale(Record r, long generation) => Reconcile is { } f && r.Int64(f.AttemptedGeneration) < generation;

    /// <summary>
    /// Due and not held: claimability without limits. On reconciliation sources a due time null means the record must
    /// not run, the backoff floor beats an invalidation and a stale generation makes an eligible record due once; on
    /// superseding sources a lease of an obsolete generation counts as not held. On job sources the record must be
    /// eligible, and a job with a calendar starts only inside the window planned with its due time; a takeover of a lost
    /// lease continues an execution that started inside it.
    /// </summary>
    public bool Claimable(Record r, DateTimeOffset now, long generation, Func<PauseKey, bool> paused)
    {
        var due = Due(r);
        var expires = Expires(r);
        var unheld = expires is null || expires <= now - TakeoverBackoff(r);
        if (Job is not null)
        {
            return Eligible(r, paused) && due is not null && due <= now && unheld && (r.Time(JobColumns.WindowEnd) is not { } end || end > now || expires is not null);
        }
        if (Reconcile is not { } f)
        {
            return due is not null && due <= now && unheld;
        }
        var stale = Stale(r, generation);
        return (r.Time(f.NotBefore) is not { } notBefore || notBefore <= now)
            && ((due is not null && due <= now) || stale)
            && (unheld || (Supersedes && stale));
    }

    /// <summary>The record has an outstanding lease of an obsolete generation, which a claim retires as a supersession.</summary>
    public bool Superseded(Record r, long generation) => Supersedes && Expires(r) is not null && Stale(r, generation);

    /// <summary>The previous lease expired and its expiry is not settled yet; a lease superseded before it expired has a live holder.</summary>
    public bool NeedsSettlement(Record r, DateTimeOffset now) =>
        Source.Settles
        && Expires(r) is { } expires
        && (!Supersedes || expires <= now)
        && r.Int64(Fields.SettledEpoch) < r.Int64(Fields.LeaseEpoch);

    /// <summary>A failed settlement waits for its retry time.</summary>
    public bool SettlementWaiting(Record r, DateTimeOffset now) =>
        Policy.SettleExpiry && r.Time(Fields.SettleRetryAt) is { } retry && retry > now;

    /// <summary>
    /// Claimability for a claimer with or without the source's expiry handler: one without leaves records whose expiry
    /// needs settling out, one with it leaves out an expiry waiting to retry its settlement.
    /// </summary>
    public bool ClaimableFor(LeaseCallbacks<TKey> callbacks, Record r, DateTimeOffset now, long generation, Func<PauseKey, bool> paused) =>
        Claimable(r, now, generation, paused)
        && (!Policy.SettleExpiry
            || (callbacks.Expired is null ? !NeedsSettlement(r, now) : !(NeedsSettlement(r, now) && SettlementWaiting(r, now))));

    /// <summary>Claimed and not yet released, ended, taken over or settled; only records in scope count.</summary>
    public bool Outstanding(Record r, DateTimeOffset now) =>
        InScope(r) && Expires(r) is { } expires && (Source.Settles ? r.Int64(Fields.SettledEpoch) < r.Int64(Fields.LeaseEpoch) : expires > now);

    /// <summary>The lease is valid for the owner and epoch; on superseding sources it must also be of the current generation.</summary>
    public bool Valid(Record r, OwnerId owner, long epoch, DateTimeOffset now, long generation) =>
        r.Uuid(Fields.LeaseOwner) == owner.Value
        && r.Int64(Fields.LeaseEpoch) == epoch
        && Expires(r) > now
        && (!Supersedes || r.Int64(Reconcile!.AttemptedGeneration) == generation);

    /// <summary>The due time a <see cref="Due"/> gives a record whose due time is <paramref name="current"/>.</summary>
    public static DateTimeOffset? DueAt(Due due, DateTimeOffset? current, DateTimeOffset now) => due.Kind switch
    {
        DueKind.Unchanged => current,
        DueKind.After => now + due.Delay,
        _ => null,
    };

    /// <summary>A due time that wakes claimers: one that is set.</summary>
    public static bool Wakes(DateTimeOffset? due) => due is not null;

    private sealed class KeyWriter : IKeyWriter
    {
        public List<object?> Values { get; } = [];

        public void Write(string value) => Values.Add(value);

        public void Write(int value) => Values.Add(value);

        public void Write(long value) => Values.Add(value);

        public void Write(Guid value) => Values.Add(value);
    }

    private sealed class KeyReader(IReadOnlyList<object> values) : IKeyReader
    {
        private int next;

        public string ReadString() => (string)values[next++];

        public int ReadInt32() => Convert.ToInt32(values[next++], CultureInfo.InvariantCulture);

        public long ReadInt64() => Convert.ToInt64(values[next++], CultureInfo.InvariantCulture);

        public Guid ReadGuid() => (Guid)values[next++];
    }
}
