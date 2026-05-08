using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData;

/// <summary>
/// Sink connector that writes to SAP OData services.
/// </summary>
[ConnectorMetadata(
    Name = "sap-odata-sink",
    Description = "Writes entities to SAP OData v2/v4 services",
    Author = "Surgewave",
    Tags = "sap, odata, rest, api, erp, sink")]
public sealed class ODataSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(ODataConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(ODataConnectorConfig.ServiceUrl, ConfigType.String, Importance.High,
            "OData service URL")
        .Define(ODataConnectorConfig.Username, ConfigType.String, "", Importance.Medium,
            "Username for authentication")
        .Define(ODataConnectorConfig.Password, ConfigType.Password, "", Importance.Medium,
            "Password for authentication")
        .Define(ODataConnectorConfig.AuthType, ConfigType.String,
            ODataConnectorConfig.DefaultAuthType, Importance.Medium,
            "Authentication type: basic, oauth", EditorHint.Select, options: ["basic", "oauth", "none"])
        .Define(ODataConnectorConfig.IgnoreCertificateErrors, ConfigType.Boolean, "false", Importance.Low,
            "Ignore SSL certificate errors")
        .Define(ODataConnectorConfig.TimeoutSeconds, ConfigType.Int,
            ODataConnectorConfig.DefaultTimeoutSeconds.ToString(), Importance.Low,
            "Request timeout in seconds")
        .Define(ODataConnectorConfig.TargetEntitySet, ConfigType.String, Importance.High,
            "Target OData entity set")
        .Define(ODataConnectorConfig.WriteMode, ConfigType.String,
            ODataConnectorConfig.DefaultWriteMode, Importance.Medium,
            "Write mode: create, update, patch, delete", EditorHint.Select, options: ["create", "update", "upsert", "delete"])
        .Define(ODataConnectorConfig.KeyFields, ConfigType.List, "", Importance.Medium,
            "Key fields for update/delete (comma-separated)")
        .Define(ODataConnectorConfig.BatchSize, ConfigType.Int,
            ODataConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for operations")
        .Define(ODataConnectorConfig.UseBatch, ConfigType.Boolean, "true", Importance.Medium,
            "Use OData batch requests");

    public override Type TaskClass => typeof(ODataSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ODataConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(ODataConnectorConfig.ServiceUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.ServiceUrl}' is required");
        }

        if (!config.TryGetValue(ODataConnectorConfig.TargetEntitySet, out var entitySet) ||
            string.IsNullOrWhiteSpace(entitySet))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.TargetEntitySet}' is required");
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
