namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune;

/// <summary>
/// Configuration constants for AWS Neptune connector.
/// </summary>
public static class NeptuneConnectorConfig
{
    public const string Endpoint = "neptune.endpoint";
    public const string Port = "neptune.port";
    public const string EnableSsl = "neptune.ssl.enabled";
    public const string Topic = "topic";

    // Source settings
    public const string Query = "neptune.query";
    public const string PollIntervalMs = "neptune.poll.interval.ms";
    public const string TraversalSource = "neptune.traversal.source";

    // Sink settings
    public const string VertexLabel = "neptune.vertex.label";
    public const string EdgeLabel = "neptune.edge.label";
    public const string WriteMode = "neptune.write.mode";
    public const string IdField = "neptune.id.field";
    public const string FromField = "neptune.from.field";
    public const string ToField = "neptune.to.field";

    // Defaults
    public const int DefaultPort = 8182;
    public const bool DefaultEnableSsl = true;
    public const int DefaultPollIntervalMs = 10000;
    public const string DefaultTraversalSource = "g";
    public const string DefaultWriteMode = "vertex";
}
