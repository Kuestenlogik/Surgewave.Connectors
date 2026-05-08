using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh;

/// <summary>
/// Sink connector that writes to SAP Event Mesh.
/// </summary>
[ConnectorMetadata(
    Name = "sap-eventmesh-sink",
    Description = "Publishes events to SAP Event Mesh enterprise messaging service",
    Author = "Surgewave",
    Tags = "sap, eventmesh, cloudevents, messaging, btp, sink")]
public sealed class EventMeshSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(EventMeshConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(EventMeshConnectorConfig.ServiceUrl, ConfigType.String, Importance.High,
            "Event Mesh messaging service URL")
        .Define(EventMeshConnectorConfig.TokenUrl, ConfigType.String, Importance.High,
            "OAuth token endpoint URL")
        .Define(EventMeshConnectorConfig.ClientId, ConfigType.String, Importance.High,
            "OAuth client ID")
        .Define(EventMeshConnectorConfig.ClientSecret, ConfigType.Password, Importance.High,
            "OAuth client secret")
        .Define(EventMeshConnectorConfig.TargetTopic, ConfigType.String, Importance.High,
            "Event Mesh topic to publish to", EditorHint.Topic)
        .Define(EventMeshConnectorConfig.Namespace, ConfigType.String, "", Importance.Medium,
            "Event Mesh namespace")
        .Define(EventMeshConnectorConfig.ContentType, ConfigType.String,
            EventMeshConnectorConfig.DefaultContentType, Importance.Medium,
            "Content type for messages")
        .Define(EventMeshConnectorConfig.CloudEventSource, ConfigType.String, "", Importance.Medium,
            "CloudEvent source URI")
        .Define(EventMeshConnectorConfig.CloudEventType, ConfigType.String, "", Importance.Medium,
            "CloudEvent type")
        .Define(EventMeshConnectorConfig.BatchSize, ConfigType.Int,
            EventMeshConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for publishing");

    public override Type TaskClass => typeof(EventMeshSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(EventMeshConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(EventMeshConnectorConfig.ServiceUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.ServiceUrl}' is required");
        }

        if (!config.TryGetValue(EventMeshConnectorConfig.TargetTopic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.TargetTopic}' is required");
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
