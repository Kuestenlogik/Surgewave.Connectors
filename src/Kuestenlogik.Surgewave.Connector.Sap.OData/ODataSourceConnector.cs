using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData;

/// <summary>
/// Source connector that reads from SAP OData services.
/// </summary>
[ConnectorMetadata(
    Name = "sap-odata-source",
    Description = "Reads entities from SAP OData v2/v4 services",
    Author = "Surgewave",
    Tags = "sap, odata, rest, api, erp, source")]
public sealed class ODataSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(ODataConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce entities to", EditorHint.Topic)
        .Define(ODataConnectorConfig.ServiceUrl, ConfigType.String, Importance.High,
            "OData service URL")
        .Define(ODataConnectorConfig.Username, ConfigType.String, "", Importance.Medium,
            "Username for authentication")
        .Define(ODataConnectorConfig.Password, ConfigType.Password, "", Importance.Medium,
            "Password for authentication")
        .Define(ODataConnectorConfig.AuthType, ConfigType.String,
            ODataConnectorConfig.DefaultAuthType, Importance.Medium,
            "Authentication type: basic, oauth, sap_assertion", EditorHint.Select, options: ["basic", "oauth", "none"])
        .Define(ODataConnectorConfig.OAuthTokenUrl, ConfigType.String, "", Importance.Low,
            "OAuth token endpoint URL")
        .Define(ODataConnectorConfig.OAuthClientId, ConfigType.String, "", Importance.Low,
            "OAuth client ID")
        .Define(ODataConnectorConfig.OAuthClientSecret, ConfigType.Password, "", Importance.Low,
            "OAuth client secret")
        .Define(ODataConnectorConfig.IgnoreCertificateErrors, ConfigType.Boolean, "false", Importance.Low,
            "Ignore SSL certificate errors")
        .Define(ODataConnectorConfig.TimeoutSeconds, ConfigType.Int,
            ODataConnectorConfig.DefaultTimeoutSeconds.ToString(), Importance.Low,
            "Request timeout in seconds")
        .Define(ODataConnectorConfig.EntitySet, ConfigType.String, Importance.High,
            "OData entity set to read")
        .Define(ODataConnectorConfig.Select, ConfigType.List, "", Importance.Medium,
            "Fields to select ($select)")
        .Define(ODataConnectorConfig.Filter, ConfigType.String, "", Importance.Medium,
            "OData filter expression ($filter)", EditorHint.Code, "odata")
        .Define(ODataConnectorConfig.Expand, ConfigType.List, "", Importance.Medium,
            "Navigation properties to expand ($expand)")
        .Define(ODataConnectorConfig.OrderBy, ConfigType.String, "", Importance.Medium,
            "Order by expression ($orderby)")
        .Define(ODataConnectorConfig.Top, ConfigType.Int,
            ODataConnectorConfig.DefaultTop.ToString(), Importance.Medium,
            "Maximum entities per poll ($top)")
        .Define(ODataConnectorConfig.IncrementalField, ConfigType.String, "", Importance.Medium,
            "Field for incremental reads")
        .Define(ODataConnectorConfig.PollIntervalMs, ConfigType.Int,
            ODataConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(ODataConnectorConfig.DeltaLink, ConfigType.Boolean, "false", Importance.Medium,
            "Use OData delta links for change tracking");

    public override Type TaskClass => typeof(ODataSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ODataConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(ODataConnectorConfig.ServiceUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.ServiceUrl}' is required");
        }

        if (!config.TryGetValue(ODataConnectorConfig.EntitySet, out var entitySet) ||
            string.IsNullOrWhiteSpace(entitySet))
        {
            throw new ArgumentException($"'{ODataConnectorConfig.EntitySet}' is required");
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
