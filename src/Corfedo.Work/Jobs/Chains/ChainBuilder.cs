namespace Corfedo.Work;

/// <summary>
/// Builds a chain: its root with <see cref="StartWith"/>, then steps with <see cref="ChainStep.Then{TArgs}(JobDefinition{TArgs}, TArgs, JobRequestOptions?)"/>
/// and groups with <see cref="ChainStep.ThenGroup"/>. Every step is a job of its own definition, which this process
/// must register; steps never merge.
/// </summary>
public sealed class ChainBuilder
{
    private readonly List<ChainStepData> steps = [];
    private readonly List<ChainGroupData> groups = [];
    private readonly List<JobDefinition> definitions = [];
    private readonly Schedule root;
    private readonly JobRegistry registry;

    internal ChainBuilder(JobRegistry registry, Schedule root)
    {
        this.registry = registry;
        this.root = root;
    }

    internal IReadOnlyList<ChainStepData> Steps => steps;

    internal IReadOnlyList<ChainGroupData> Groups => groups;

    internal IReadOnlyList<JobDefinition> Definitions => definitions;

    /// <summary>The chain's root step, due by the chain's schedule; a chain has one.</summary>
    public ChainStep StartWith<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null)
    {
        if (steps.Count > 0)
        {
            throw new InvalidOperationException("A chain starts with one root step.");
        }
        return Add(definition, args, options, null, null, ChainOutcomes.Any, root);
    }

    internal ChainStep Add<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options, Guid? parent, Guid? group, ChainOutcomes condition, Schedule? schedule = null)
    {
        ArgumentNullException.ThrowIfNull(definition);
        options ??= JobRequestOptions.Default;
        if (options.MergeKey is not null)
        {
            throw new ArgumentException("Chain steps never merge; a merge key is not used.", nameof(options));
        }
        var id = JobId.New();
        steps.Add(new ChainStepData(id, definition.Key, JobOperations.Data(registry.Args(definition), definition, args, schedule ?? Schedule.Now, options with { Mode = RequestMode.New }), parent, group, condition));
        definitions.Add(definition);
        return new ChainStep(this, id, group);
    }

    internal Guid AddGroup(Guid parent, Guid? group)
    {
        var id = Guid.CreateVersion7();
        groups.Add(new ChainGroupData(id, parent, group));
        return id;
    }

    internal static ChainOutcomes Outcomes(RunCondition condition) => condition switch
    {
        RunCondition.OnSuccess => ChainOutcomes.Succeeded,
        RunCondition.OnFailure => ChainOutcomes.Failed,
        RunCondition.OnCancelled => ChainOutcomes.Cancelled,
        RunCondition.OnFailureOrCancelled => ChainOutcomes.Failed | ChainOutcomes.Cancelled,
        RunCondition.OnAnyOutcome => ChainOutcomes.Any,
        _ => throw new ArgumentOutOfRangeException(nameof(condition)),
    };
}

/// <summary>A step of a chain being built; further steps and groups follow it.</summary>
public sealed class ChainStep
{
    private readonly ChainBuilder builder;
    private readonly Guid? group;

    internal ChainStep(ChainBuilder builder, JobId id, Guid? group)
    {
        this.builder = builder;
        this.group = group;
        Id = id;
    }

    /// <summary>The step's job id.</summary>
    public JobId Id { get; }

    /// <summary>A step that runs once this one completed.</summary>
    public ChainStep Then<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        builder.Add(definition, args, options, Id.Value, group, ChainOutcomes.Succeeded);

    /// <summary>A step that runs once this one settled as <paramref name="condition"/> says; otherwise it is skipped.</summary>
    public ChainStep Then<TArgs>(JobDefinition<TArgs> definition, TArgs args, RunCondition condition, JobRequestOptions? options = null) =>
        builder.Add(definition, args, options, Id.Value, group, ChainBuilder.Outcomes(condition));

    /// <summary>A group whose branches follow this step; continuations added to the group run once every branch settled.</summary>
    public ChainGroup ThenGroup(Action<ChainGroupBuilder> branches)
    {
        ArgumentNullException.ThrowIfNull(branches);
        var id = builder.AddGroup(Id.Value, group);
        branches(new ChainGroupBuilder(builder, id, Id));
        return new ChainGroup(builder, id, group);
    }
}

/// <summary>Adds the branches of a group. A branch starts with a step that follows the group's step.</summary>
public sealed class ChainGroupBuilder
{
    private readonly ChainBuilder builder;
    private readonly Guid group;
    private readonly JobId parent;

    internal ChainGroupBuilder(ChainBuilder builder, Guid group, JobId parent)
    {
        this.builder = builder;
        this.group = group;
        this.parent = parent;
    }

    /// <summary>A branch whose first step runs once the group's step completed.</summary>
    public ChainStep Add<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        builder.Add(definition, args, options, parent.Value, group, ChainOutcomes.Succeeded);

    /// <summary>A branch whose first step runs once the group's step settled as <paramref name="condition"/> says.</summary>
    public ChainStep Add<TArgs>(JobDefinition<TArgs> definition, TArgs args, RunCondition condition, JobRequestOptions? options = null) =>
        builder.Add(definition, args, options, parent.Value, group, ChainBuilder.Outcomes(condition));

    /// <summary>One single-step branch per item, each running once the group's step completed.</summary>
    public ChainGroupBuilder AddMany<TArgs>(JobDefinition<TArgs> definition, IEnumerable<JobRequestItem<TArgs>> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
        {
            builder.Add(definition, item.Args, item.Options, parent.Value, group, ChainOutcomes.Succeeded);
        }
        return this;
    }
}

/// <summary>
/// A group of a chain being built. Its continuations run once every branch settled, by the group's aggregate outcome:
/// failed when any branch failed or timed out, otherwise cancelled when any was cancelled, otherwise succeeded when any
/// succeeded or the group is empty, otherwise skipped.
/// </summary>
public sealed class ChainGroup
{
    private readonly ChainBuilder builder;
    private readonly Guid? group;

    internal ChainGroup(ChainBuilder builder, Guid id, Guid? group)
    {
        this.builder = builder;
        this.group = group;
        Id = id;
    }

    /// <summary>The group's id.</summary>
    public Guid Id { get; }

    /// <summary>A continuation that runs when the group succeeded.</summary>
    public ChainGroup AfterAll<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        After(definition, args, options, ChainOutcomes.Succeeded);

    /// <summary>A continuation that runs when the group failed.</summary>
    public ChainGroup AfterAnyFailed<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        After(definition, args, options, ChainOutcomes.Failed);

    /// <summary>A continuation that runs when the group failed or was cancelled.</summary>
    public ChainGroup AfterAnyFailedOrCancelled<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        After(definition, args, options, ChainOutcomes.Failed | ChainOutcomes.Cancelled);

    /// <summary>A continuation that runs once the group settled, whatever the outcome.</summary>
    public ChainGroup AfterAllSettled<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options = null) =>
        After(definition, args, options, ChainOutcomes.Any);

    private ChainGroup After<TArgs>(JobDefinition<TArgs> definition, TArgs args, JobRequestOptions? options, ChainOutcomes condition)
    {
        builder.Add(definition, args, options, Id, group, condition);
        return this;
    }
}
