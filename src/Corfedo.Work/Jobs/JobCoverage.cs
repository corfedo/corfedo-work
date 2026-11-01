namespace Corfedo.Work;

/// <summary>
/// A job definition as one registered process registers it: the revision of its own code, whether it runs the handler
/// and the argument versions it then accepts, and whether it declares the definition's code-defined recurrence.
/// </summary>
public sealed record ProcessDefinition(string Definition, long Revision, bool Runs, IReadOnlyList<int> AcceptedArgs, bool DeclaresRecurrence);

/// <summary>
/// Due work no live process can run (R12, R21): definitions no live process runs, and argument versions of definitions
/// that live processes run but none accepts. A definition appears in one list at most.
/// </summary>
public sealed record UnservedReport(IReadOnlyList<UnservedDefinition> Definitions, IReadOnlyList<UnservedVersion> Versions);

/// <summary>A definition with due work that no live process runs.</summary>
public sealed record UnservedDefinition(string Definition);

/// <summary>An argument version of due jobs that no live process running the definition accepts.</summary>
public sealed record UnservedVersion(string Definition, int ArgsVersion);

/// <summary>A code-defined recurrence that no live process declares any longer (J10); it is kept until removed explicitly.</summary>
public sealed record UndeclaredRecurrence(string Definition, long DeclaredRevision, DateTimeOffset? NextAt);

/// <summary>A live process whose own revision of a definition is older than the stored one (R18).</summary>
public sealed record OutdatedProcess(ProcessInstance Process, string Definition, long Revision, long StoredRevision);
