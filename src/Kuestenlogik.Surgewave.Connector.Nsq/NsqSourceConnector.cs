using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nsq;

/// <summary>
/// Source connector that consumes messages from NSQ topics/channels.
/// Supports both direct nsqd connection and nsqlookupd discovery.
/// </summary>
[ConnectorMetadata(
    Name = "NSQ Source",
    Description = "Consumes messages from NSQ topics/channels. Supports nsqd and nsqlookupd connections.",
    Author = "KL Surgewave",
    Tags = "nsq,messaging,queue,source",
    Icon = "MessageProcessing")]
public sealed class NsqSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(NsqSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(NsqConnectorConfig.NsqdAddress, ConfigType.String, NsqConnectorConfig.DefaultNsqdAddress, Importance.Medium, "Direct nsqd TCP address (host:port)")
        .Define(NsqConnectorConfig.NsqLookupdAddresses, ConfigType.String, "", Importance.Medium, "Comma-separated nsqlookupd HTTP addresses (host:port)")
        .Define(NsqConnectorConfig.NsqTopic, ConfigType.String, Importance.High, "NSQ topic to consume from", EditorHint.Topic)
        .Define(NsqConnectorConfig.NsqChannel, ConfigType.String, NsqConnectorConfig.DefaultChannel, Importance.High, "NSQ channel name")
        .Define(NsqConnectorConfig.Topic, ConfigType.String, Importance.High, "Target Surgewave topic", EditorHint.Topic)
        .Define(NsqConnectorConfig.MaxInFlight, ConfigType.Int, NsqConnectorConfig.DefaultMaxInFlight, Importance.Medium, "Maximum number of messages in flight")
        .Define(NsqConnectorConfig.MaxAttempts, ConfigType.Int, NsqConnectorConfig.DefaultMaxAttempts, Importance.Low, "Maximum number of attempts per message")
        .Define(NsqConnectorConfig.RequeueDelayMs, ConfigType.Int, NsqConnectorConfig.DefaultRequeueDelayMs, Importance.Low, "Requeue delay in milliseconds")
        .Define(NsqConnectorConfig.BatchSize, ConfigType.Int, NsqConnectorConfig.DefaultBatchSize, Importance.Medium, "Batch size for polling")
        .Define(NsqConnectorConfig.PollTimeoutMs, ConfigType.Int, NsqConnectorConfig.DefaultPollTimeoutMs, Importance.Medium, "Poll timeout in milliseconds")
        .Define(NsqConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium, "Enable TLS")
        .Define(NsqConnectorConfig.TlsInsecureSkipVerify, ConfigType.Boolean, false, Importance.Low, "Skip TLS certificate verification")
        .Define(NsqConnectorConfig.AuthSecret, ConfigType.Password, "", Importance.Low, "NSQ auth secret");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(NsqConnectorConfig.NsqTopic, out var nsqTopic) || string.IsNullOrEmpty(nsqTopic))
            throw new ArgumentException($"Required configuration '{NsqConnectorConfig.NsqTopic}' is missing");

        if (!config.TryGetValue(NsqConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Required configuration '{NsqConnectorConfig.Topic}' is missing");

        // Validate at least one connection method is configured
        var hasNsqd = config.TryGetValue(NsqConnectorConfig.NsqdAddress, out var nsqd) && !string.IsNullOrEmpty(nsqd);
        var hasLookupd = config.TryGetValue(NsqConnectorConfig.NsqLookupdAddresses, out var lookupd) && !string.IsNullOrEmpty(lookupd);

        if (!hasNsqd && !hasLookupd)
            throw new ArgumentException($"At least one of '{NsqConnectorConfig.NsqdAddress}' or '{NsqConnectorConfig.NsqLookupdAddresses}' must be configured");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // NSQ consumer per channel, single task
        return [new Dictionary<string, string>(_config)];
    }
}
