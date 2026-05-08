using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune;

/// <summary>
/// Sink connector for writing to AWS Neptune graph database.
/// </summary>
[ConnectorMetadata(
    Name = "neptune-sink",
    Description = "Writes vertices and edges to AWS Neptune graph database",
    Author = "Surgewave",
    Tags = "aws,neptune,graph,gremlin,sink")]
public sealed class NeptuneSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(NeptuneSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NeptuneConnectorConfig.Endpoint, ConfigType.String, Importance.High, "Neptune cluster endpoint")
        .Define(NeptuneConnectorConfig.Port, ConfigType.Int, NeptuneConnectorConfig.DefaultPort, Importance.Medium, "Neptune port (default: 8182)")
        .Define(NeptuneConnectorConfig.EnableSsl, ConfigType.Boolean, NeptuneConnectorConfig.DefaultEnableSsl, Importance.Medium, "Enable SSL connection")
        .Define(NeptuneConnectorConfig.WriteMode, ConfigType.String, NeptuneConnectorConfig.DefaultWriteMode, Importance.High, "Write mode: vertex or edge", EditorHint.Select, options: ["vertex", "edge"])
        .Define(NeptuneConnectorConfig.VertexLabel, ConfigType.String, "vertex", Importance.Medium, "Label for created vertices")
        .Define(NeptuneConnectorConfig.EdgeLabel, ConfigType.String, "edge", Importance.Medium, "Label for created edges")
        .Define(NeptuneConnectorConfig.IdField, ConfigType.String, "id", Importance.Medium, "Field containing vertex/edge ID")
        .Define(NeptuneConnectorConfig.FromField, ConfigType.String, "from", Importance.Medium, "Field containing source vertex ID for edges")
        .Define(NeptuneConnectorConfig.ToField, ConfigType.String, "to", Importance.Medium, "Field containing target vertex ID for edges");

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
