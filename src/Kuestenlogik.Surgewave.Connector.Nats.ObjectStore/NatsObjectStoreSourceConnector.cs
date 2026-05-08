using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore;

/// <summary>
/// Source connector that watches NATS JetStream Object Store for changes.
/// </summary>
[ConnectorMetadata(
    Name = "nats-objectstore-source",
    Description = "Watches NATS JetStream Object Store for object changes",
    Author = "Surgewave",
    Tags = "nats, jetstream, objectstore, blob, source")]
public sealed class NatsObjectStoreSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsObjectStoreConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce object events to", EditorHint.Topic)
        .Define(NatsObjectStoreConnectorConfig.Servers, ConfigType.List, Importance.High,
            "NATS server URLs (comma-separated)")
        .Define(NatsObjectStoreConnectorConfig.BucketName, ConfigType.String, Importance.High,
            "Object Store bucket name")
        .Define(NatsObjectStoreConnectorConfig.Username, ConfigType.String, "", Importance.Medium,
            "NATS username")
        .Define(NatsObjectStoreConnectorConfig.Password, ConfigType.Password, "", Importance.Medium,
            "NATS password")
        .Define(NatsObjectStoreConnectorConfig.Token, ConfigType.Password, "", Importance.Medium,
            "NATS auth token")
        .Define(NatsObjectStoreConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.Medium,
            "Path to NATS credentials file", EditorHint.FilePath)
        .Define(NatsObjectStoreConnectorConfig.WatchPrefix, ConfigType.String, "", Importance.Medium,
            "Watch only objects with this prefix")
        .Define(NatsObjectStoreConnectorConfig.IncludeHistory, ConfigType.Boolean, "false", Importance.Medium,
            "Include historical entries on startup")
        .Define(NatsObjectStoreConnectorConfig.IncludeDeletes, ConfigType.Boolean, "true", Importance.Medium,
            "Include delete events")
        .Define(NatsObjectStoreConnectorConfig.IncludeContent, ConfigType.Boolean, "true", Importance.Medium,
            "Include object content in events")
        .Define(NatsObjectStoreConnectorConfig.MaxContentSize, ConfigType.Int,
            NatsObjectStoreConnectorConfig.DefaultMaxContentSize.ToString(), Importance.Low,
            "Maximum content size to include (bytes)");

    public override Type TaskClass => typeof(NatsObjectStoreSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(NatsObjectStoreConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{NatsObjectStoreConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(NatsObjectStoreConnectorConfig.BucketName, out var bucket) ||
            string.IsNullOrWhiteSpace(bucket))
        {
            throw new ArgumentException($"'{NatsObjectStoreConnectorConfig.BucketName}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
