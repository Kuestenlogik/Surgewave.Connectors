using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Orleans;

/// <summary>
/// A sink connector that publishes Surgewave records to Microsoft Orleans Grain Streams.
/// </summary>
[ConnectorMetadata(
    Name = "Orleans Stream Sink",
    Description = "Publish Surgewave records to Microsoft Orleans Grain Streams",
    Tags = "orleans,grains,streams,actors",
    Icon = "CloudCircle")]
public sealed class OrleansSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(OrleansSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(OrleansConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Surgewave topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(OrleansConnectorConfig.ClusterUrl, ConfigType.String,
            OrleansConnectorConfig.DefaultClusterUrl, Importance.High,
            "Orleans gateway address (host:port)")
        .Define(OrleansConnectorConfig.ClusterId, ConfigType.String,
            OrleansConnectorConfig.DefaultClusterId, Importance.Medium,
            "Orleans cluster identifier")
        .Define(OrleansConnectorConfig.ServiceId, ConfigType.String,
            OrleansConnectorConfig.DefaultServiceId, Importance.Medium,
            "Orleans service identifier")
        .Define(OrleansConnectorConfig.StreamProvider, ConfigType.String,
            OrleansConnectorConfig.DefaultStreamProvider, Importance.Medium,
            "Orleans stream provider name")
        .Define(OrleansConnectorConfig.StreamNamespace, ConfigType.String, Importance.High,
            "Orleans stream namespace")
        .Define(OrleansConnectorConfig.StreamId, ConfigType.String,
            "", Importance.Medium,
            "Orleans stream GUID (derived from topic name if not provided)")
        .Define(OrleansConnectorConfig.SerializationType, ConfigType.String,
            OrleansConnectorConfig.DefaultSerializationType, Importance.Low,
            "Serialization type: json or binary")
        .Define(OrleansConnectorConfig.PublishTimeoutMs, ConfigType.Int,
            OrleansConnectorConfig.DefaultPublishTimeoutMs, Importance.Low,
            "Publish timeout in milliseconds")
        .Define(OrleansConnectorConfig.Retries, ConfigType.Int,
            OrleansConnectorConfig.DefaultRetries, Importance.Low,
            "Number of publish retries on failure");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(OrleansConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {OrleansConnectorConfig.Topics}");
        if (!config.ContainsKey(OrleansConnectorConfig.StreamNamespace))
            throw new ArgumentException($"Missing required config: {OrleansConnectorConfig.StreamNamespace}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Orleans stream publisher runs as a single task
        return [new Dictionary<string, string>(_config)];
    }
}
