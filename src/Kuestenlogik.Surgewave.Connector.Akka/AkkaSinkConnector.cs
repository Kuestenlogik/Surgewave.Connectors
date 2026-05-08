using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Sink connector that sends messages to Akka.NET actors.
/// Supports both Tell (fire-and-forget) and Ask (request-response) patterns.
/// </summary>
public sealed class AkkaSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(AkkaSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(AkkaConnectorConfig.ActorSystemNameConfig, ConfigType.String, AkkaConnectorConfig.DefaultActorSystemName, Importance.Medium, "Actor system name")
        .Define(AkkaConnectorConfig.ActorSystemConfigConfig, ConfigType.String, "", Importance.Medium, "HOCON configuration for the actor system", EditorHint.Multiline)
        .Define(AkkaConnectorConfig.ActorPathConfig, ConfigType.String, Importance.High, "Target actor path (e.g., /user/processor)")
        .Define(AkkaConnectorConfig.RemoteAddressConfig, ConfigType.String, "", Importance.Medium, "Remote actor system address (akka.tcp://system@host:port)")
        .Define(AkkaConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(AkkaConnectorConfig.AskTimeoutMsConfig, ConfigType.Int, (int)AkkaConnectorConfig.DefaultAskTimeoutMs, Importance.Low, "Ask timeout in milliseconds (when tell.only=false)")
        .Define(AkkaConnectorConfig.TellOnlyConfig, ConfigType.Boolean, true, Importance.Low, "Use Tell (fire-and-forget) instead of Ask")
        .Define(AkkaConnectorConfig.BatchSizeConfig, ConfigType.Int, AkkaConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for processing")
        .Define(AkkaConnectorConfig.MaxRetryCountConfig, ConfigType.Int, AkkaConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for failures")
        .Define(AkkaConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)AkkaConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(AkkaConnectorConfig.ActorPathConfig, out var actorPath) || string.IsNullOrEmpty(actorPath))
            throw new ArgumentException($"Required configuration '{AkkaConnectorConfig.ActorPathConfig}' is missing");

        if (!config.TryGetValue(AkkaConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{AkkaConnectorConfig.TopicsConfig}' is missing");

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
