namespace Kuestenlogik.Surgewave.Connector.Mirror.Policies;

/// <summary>
/// Factory for creating replication policies based on configuration.
/// </summary>
public static class ReplicationPolicyFactory
{
    /// <summary>
    /// Create a replication policy from configuration.
    /// </summary>
    public static IReplicationPolicy Create(MirrorMakerConfig config)
    {
        return Create(config.ReplicationPolicyClass, config.ReplicationPolicySeparator);
    }

    /// <summary>
    /// Create a replication policy by class name.
    /// </summary>
    public static IReplicationPolicy Create(string className, string separator = ".")
    {
        return className switch
        {
            "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy" or
            "DefaultReplicationPolicy" or
            "default" => new DefaultReplicationPolicy(separator),

            "Kuestenlogik.Surgewave.Connect.Mirror.Policies.IdentityReplicationPolicy" or
            "IdentityReplicationPolicy" or
            "identity" => new IdentityReplicationPolicy(),

            _ => CreateFromType(className, separator)
        };
    }

    private static IReplicationPolicy CreateFromType(string className, string separator)
    {
        var type = Type.GetType(className);
        if (type == null)
            throw new ArgumentException($"Unknown replication policy class: {className}");

        if (!typeof(IReplicationPolicy).IsAssignableFrom(type))
            throw new ArgumentException($"Class {className} does not implement IReplicationPolicy");

        // Try constructor with separator parameter
        var ctor = type.GetConstructor([typeof(string)]);
        if (ctor != null)
            return (IReplicationPolicy)ctor.Invoke([separator]);

        // Try parameterless constructor
        return (IReplicationPolicy)Activator.CreateInstance(type)!;
    }
}
