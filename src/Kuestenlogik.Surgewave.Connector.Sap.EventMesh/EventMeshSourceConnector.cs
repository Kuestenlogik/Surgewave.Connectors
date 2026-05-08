using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh;

/// <summary>
/// Source connector that reads from SAP Event Mesh.
/// </summary>
[ConnectorMetadata(
    Name = "sap-eventmesh-source",
    Description = "Reads events from SAP Event Mesh enterprise messaging service",
    Author = "Surgewave",
    Tags = "sap, eventmesh, cloudevents, messaging, btp, source")]
public sealed class EventMeshSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(EventMeshConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce events to", EditorHint.Topic)
        .Define(EventMeshConnectorConfig.ServiceUrl, ConfigType.String, Importance.High,
            "Event Mesh messaging service URL")
        .Define(EventMeshConnectorConfig.TokenUrl, ConfigType.String, Importance.High,
            "OAuth token endpoint URL")
        .Define(EventMeshConnectorConfig.ClientId, ConfigType.String, Importance.High,
            "OAuth client ID")
        .Define(EventMeshConnectorConfig.ClientSecret, ConfigType.Password, Importance.High,
            "OAuth client secret")
        .Define(EventMeshConnectorConfig.Protocol, ConfigType.String,
            EventMeshConnectorConfig.DefaultProtocol, Importance.Medium,
            "Protocol: rest, amqp, mqtt", EditorHint.Select, options: ["amqp", "mqtt", "rest"])
        .Define(EventMeshConnectorConfig.QueueName, ConfigType.String, "", Importance.High,
            "Queue name to consume from")
        .Define(EventMeshConnectorConfig.TopicPattern, ConfigType.String, "", Importance.Medium,
            "Topic subscription pattern")
        .Define(EventMeshConnectorConfig.Namespace, ConfigType.String, "", Importance.Medium,
            "Event Mesh namespace")
        .Define(EventMeshConnectorConfig.PollIntervalMs, ConfigType.Int,
            EventMeshConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(EventMeshConnectorConfig.MaxMessages, ConfigType.Int,
            EventMeshConnectorConfig.DefaultMaxMessages.ToString(), Importance.Medium,
            "Maximum messages per poll")
        .Define(EventMeshConnectorConfig.AckMode, ConfigType.String,
            EventMeshConnectorConfig.DefaultAckMode, Importance.Medium,
            "Acknowledgment mode: auto, manual", EditorHint.Select, options: ["auto", "manual"])
        .Define(EventMeshConnectorConfig.PrefetchCount, ConfigType.Int,
            EventMeshConnectorConfig.DefaultPrefetchCount.ToString(), Importance.Low,
            "Prefetch count");

    public override Type TaskClass => typeof(EventMeshSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(EventMeshConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(EventMeshConnectorConfig.ServiceUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.ServiceUrl}' is required");
        }

        if (!config.TryGetValue(EventMeshConnectorConfig.QueueName, out var queue) ||
            string.IsNullOrWhiteSpace(queue))
        {
            throw new ArgumentException($"'{EventMeshConnectorConfig.QueueName}' is required");
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
