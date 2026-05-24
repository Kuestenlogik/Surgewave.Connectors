namespace Kuestenlogik.Surgewave.Connector.Sap.OData;

/// <summary>
/// Configuration constants for SAP OData connector.
/// </summary>
public static class ODataConnectorConfig
{
    // Connection settings
    public const string ServiceUrl = "odata.service.url";
    public const string Username = "odata.username";
    public const string Password = "odata.password";
    public const string AuthType = "odata.auth.type";  // basic, oauth, sap_assertion
    public const string OAuthTokenUrl = "odata.oauth.token.url";
    public const string OAuthClientId = "odata.oauth.client.id";
    public const string OAuthClientSecret = "odata.oauth.client.secret";
    public const string IgnoreCertificateErrors = "odata.ignore.cert.errors";
    public const string TimeoutSeconds = "odata.timeout.seconds";

    // Source settings
    public const string Topic = "topic";
    public const string EntitySet = "odata.entity.set";
    public const string Select = "odata.select";  // $select fields
    public const string Filter = "odata.filter";  // $filter expression
    public const string Expand = "odata.expand";  // $expand navigation
    public const string OrderBy = "odata.orderby";  // $orderby
    public const string Top = "odata.top";  // $top limit
    public const string IncrementalField = "odata.incremental.field";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string DeltaLink = "odata.use.delta";  // Use OData delta links

    // Sink settings
    public const string Topics = "topics";
    public const string TargetEntitySet = "odata.target.entity.set";
    public const string WriteMode = "odata.write.mode";  // create, update, patch, delete
    public const string KeyFields = "odata.key.fields";
    public const string BatchSize = "odata.batch.size";
    public const string UseBatch = "odata.use.batch";

    // Defaults
    public const int DefaultPollIntervalMs = 5000;
    public const int DefaultTop = 1000;
    public const int DefaultBatchSize = 100;
    public const int DefaultTimeoutSeconds = 120;
    public const string DefaultAuthType = "basic";
    public const string DefaultWriteMode = "create";
}
