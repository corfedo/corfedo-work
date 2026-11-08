namespace Corfedo.Work;

/// <summary>How a starting process treats the store's framework schema.</summary>
public enum WorkSchemaMode
{
    /// <summary>The process applies the schema scripts its release adds, so its login may change the schema.</summary>
    Migrate,

    /// <summary>
    /// The process changes no schema: it checks that a separate migrate step applied this release's version or a later
    /// one and that its login may use the framework tables, and refuses to start otherwise.
    /// </summary>
    Verify,
}
