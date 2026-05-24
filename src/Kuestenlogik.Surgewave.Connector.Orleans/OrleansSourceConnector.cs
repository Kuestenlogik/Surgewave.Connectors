using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Orleans;

/// <summary>
/// A source connector that consumes events from Microsoft Orleans Grain Streams
/// and produces them to Surgewave topics.
/// </summary>
[ConnectorMetadata(
    Name = "Orleans Stream Source",
    Description = "Consume events from Microsoft Orleans Grain Streams",
    Tags = "orleans,grains,streams,actors",
    Icon = "CloudCircle")]
public sealed class OrleansSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(OrleansSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(OrleansConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to write records to", EditorHint.Topic)
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
        .Define(OrleansConnectorConfig.BatchSize, ConfigType.Int,
            OrleansConnectorConfig.DefaultBatchSize, Importance.Low,
            "Maximum records per poll")
        .Define(OrleansConnectorConfig.SerializationType, ConfigType.String,
            OrleansConnectorConfig.DefaultSerializationType, Importance.Low,
            "Serialization type: json or binary");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(OrleansConnectorConfig.Topic))
            throw new ArgumentException($"Missing required config: {OrleansConnectorConfig.Topic}");
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
        // Orleans stream subscription uses a single task
        return [new Dictionary<string, string>(_config)];
    }
}
