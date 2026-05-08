using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nsq;

/// <summary>
/// Sink connector that publishes messages to NSQ topics.
/// Connects directly to nsqd for publishing.
/// </summary>
[ConnectorMetadata(
    Name = "NSQ Sink",
    Description = "Publishes messages to NSQ topics. Connects directly to nsqd for publishing.",
    Author = "KL Surgewave",
    Tags = "nsq,messaging,queue,sink",
    Icon = "MessageReply")]
public sealed class NsqSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(NsqSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(NsqConnectorConfig.NsqdAddress, ConfigType.String, NsqConnectorConfig.DefaultNsqdAddress, Importance.High, "nsqd TCP address (host:port)")
        .Define(NsqConnectorConfig.NsqTopic, ConfigType.String, Importance.High, "NSQ topic to publish to", EditorHint.Topic)
        .Define(NsqConnectorConfig.Topics, ConfigType.List, Importance.High, "Surgewave topics to consume", EditorHint.Topic)
        .Define(NsqConnectorConfig.PublishTimeoutMs, ConfigType.Int, NsqConnectorConfig.DefaultPublishTimeoutMs, Importance.Low, "Publish timeout in milliseconds")
        .Define(NsqConnectorConfig.Retries, ConfigType.Int, NsqConnectorConfig.DefaultRetries, Importance.Low, "Number of publish retries on failure")
        .Define(NsqConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium, "Enable TLS")
        .Define(NsqConnectorConfig.TlsInsecureSkipVerify, ConfigType.Boolean, false, Importance.Low, "Skip TLS certificate verification")
        .Define(NsqConnectorConfig.AuthSecret, ConfigType.Password, "", Importance.Low, "NSQ auth secret");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(NsqConnectorConfig.NsqdAddress, out var nsqd) || string.IsNullOrEmpty(nsqd))
            throw new ArgumentException($"Required configuration '{NsqConnectorConfig.NsqdAddress}' is missing");

        if (!config.TryGetValue(NsqConnectorConfig.NsqTopic, out var nsqTopic) || string.IsNullOrEmpty(nsqTopic))
            throw new ArgumentException($"Required configuration '{NsqConnectorConfig.NsqTopic}' is missing");

        if (!config.TryGetValue(NsqConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{NsqConnectorConfig.Topics}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per connector for publishing
        return [new Dictionary<string, string>(_config)];
    }
}
