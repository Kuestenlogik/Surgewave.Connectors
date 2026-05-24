using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore;

/// <summary>
/// Sink connector that writes objects to NATS JetStream Object Store.
/// </summary>
[ConnectorMetadata(
    Name = "nats-objectstore-sink",
    Description = "Writes objects to NATS JetStream Object Store",
    Author = "Surgewave",
    Tags = "nats, jetstream, objectstore, blob, sink")]
public sealed class NatsObjectStoreSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsObjectStoreConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(NatsObjectStoreConnectorConfig.Servers, ConfigType.List, Importance.High,
            "NATS server URLs (comma-separated)")
        .Define(NatsObjectStoreConnectorConfig.BucketName, ConfigType.String, Importance.High,
            "Object Store bucket name")
        .Define(NatsObjectStoreConnectorConfig.CreateBucket, ConfigType.Boolean, "true", Importance.Medium,
            "Create bucket if it doesn't exist")
        .Define(NatsObjectStoreConnectorConfig.Username, ConfigType.String, "", Importance.Medium,
            "NATS username")
        .Define(NatsObjectStoreConnectorConfig.Password, ConfigType.Password, "", Importance.Medium,
            "NATS password")
        .Define(NatsObjectStoreConnectorConfig.Token, ConfigType.Password, "", Importance.Medium,
            "NATS auth token")
        .Define(NatsObjectStoreConnectorConfig.ObjectNameField, ConfigType.String, "name", Importance.Medium,
            "JSON field containing object name")
        .Define(NatsObjectStoreConnectorConfig.ObjectNamePrefix, ConfigType.String, "", Importance.Medium,
            "Prefix to add to object names")
        .Define(NatsObjectStoreConnectorConfig.ContentType, ConfigType.String, "", Importance.Low,
            "Content-Type header for objects")
        .Define(NatsObjectStoreConnectorConfig.ChunkSize, ConfigType.Int,
            NatsObjectStoreConnectorConfig.DefaultChunkSize.ToString(), Importance.Low,
            "Chunk size for large objects");

    public override Type TaskClass => typeof(NatsObjectStoreSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(NatsObjectStoreConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{NatsObjectStoreConnectorConfig.Topics}' is required");
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
