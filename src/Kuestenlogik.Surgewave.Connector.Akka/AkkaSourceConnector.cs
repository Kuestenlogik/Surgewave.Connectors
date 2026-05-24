using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Source connector that receives messages from Akka.NET actors.
/// Creates an inbox/receiver actor that collects messages for Surgewave topics.
/// </summary>
public sealed class AkkaSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(AkkaSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(AkkaConnectorConfig.ActorSystemNameConfig, ConfigType.String, AkkaConnectorConfig.DefaultActorSystemName, Importance.Medium, "Actor system name")
        .Define(AkkaConnectorConfig.ActorSystemConfigConfig, ConfigType.String, "", Importance.Medium, "HOCON configuration for the actor system", EditorHint.Multiline)
        .Define(AkkaConnectorConfig.ActorPathConfig, ConfigType.String, "/user/surgewave-receiver", Importance.High, "Actor path for the receiver actor")
        .Define(AkkaConnectorConfig.RemoteAddressConfig, ConfigType.String, "", Importance.Low, "Remote actor system address (akka.tcp://system@host:port)")
        .Define(AkkaConnectorConfig.TopicPatternConfig, ConfigType.String, AkkaConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${path})")
        .Define(AkkaConnectorConfig.PollTimeoutMsConfig, ConfigType.Int, (int)AkkaConnectorConfig.DefaultPollTimeoutMs, Importance.Low, "Poll timeout in milliseconds")
        .Define(AkkaConnectorConfig.MaxMessagesPerPollConfig, ConfigType.Int, AkkaConnectorConfig.DefaultMaxMessagesPerPoll, Importance.Low, "Maximum messages per poll")
        .Define(AkkaConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Akka metadata in output")
        .Define(AkkaConnectorConfig.MessageTypeConfig, ConfigType.String, "", Importance.Low, "Filter by message type name (empty = all)");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - actor system is shared
        return [new Dictionary<string, string>(_config)];
    }
}
