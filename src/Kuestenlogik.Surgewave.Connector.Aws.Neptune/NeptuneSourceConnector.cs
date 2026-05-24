using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune;

/// <summary>
/// Source connector for querying AWS Neptune graph database.
/// </summary>
[ConnectorMetadata(
    Name = "neptune-source",
    Description = "Executes Gremlin queries on AWS Neptune graph database",
    Author = "Surgewave",
    Tags = "aws,neptune,graph,gremlin,source")]
public sealed class NeptuneSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(NeptuneSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NeptuneConnectorConfig.Endpoint, ConfigType.String, Importance.High, "Neptune cluster endpoint")
        .Define(NeptuneConnectorConfig.Port, ConfigType.Int, NeptuneConnectorConfig.DefaultPort, Importance.Medium, "Neptune port (default: 8182)")
        .Define(NeptuneConnectorConfig.EnableSsl, ConfigType.Boolean, NeptuneConnectorConfig.DefaultEnableSsl, Importance.Medium, "Enable SSL connection")
        .Define(NeptuneConnectorConfig.Topic, ConfigType.String, Importance.High, "Surgewave topic to write results to", EditorHint.Topic)
        .Define(NeptuneConnectorConfig.Query, ConfigType.String, Importance.High, "Gremlin query to execute", EditorHint.Code, "gremlin")
        .Define(NeptuneConnectorConfig.PollIntervalMs, ConfigType.Int, NeptuneConnectorConfig.DefaultPollIntervalMs, Importance.Medium, "Poll interval in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
