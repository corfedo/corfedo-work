namespace Corfedo.Work;

/// <summary>This process instance: its owner id is new for every start.</summary>
public sealed class WorkProcess
{
    public OwnerId Owner { get; } = OwnerId.New();
}
