namespace Kuestenlogik.Surgewave.Connector.Sap.Hana;

/// <summary>
/// Configuration constants for SAP HANA connector.
/// </summary>
public static class HanaConnectorConfig
{
    // Connection settings
    public const string ConnectionString = "hana.connection.string";
    public const string Host = "hana.host";
    public const string Port = "hana.port";
    public const string Database = "hana.database";
    public const string Schema = "hana.schema";
    public const string Username = "hana.username";
    public const string Password = "hana.password";
    public const string UseSsl = "hana.ssl";
    public const string ValidateCertificate = "hana.ssl.validate";

    // Source settings
    public const string Topic = "topic";
    public const string Query = "hana.query";
    public const string Table = "hana.table";
    public const string Columns = "hana.columns";
    public const string IncrementalColumn = "hana.incremental.column";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string RowLimit = "hana.row.limit";

    // Sink settings
    public const string Topics = "topics";
    public const string TargetTable = "hana.target.table";
    public const string WriteMode = "hana.write.mode";  // insert, upsert, merge
    public const string KeyColumns = "hana.key.columns";
    public const string BatchSize = "hana.batch.size";

    // Defaults
    public const int DefaultPort = 30015;
    public const int DefaultPollIntervalMs = 5000;
    public const int DefaultRowLimit = 10000;
    public const int DefaultBatchSize = 1000;
    public const string DefaultWriteMode = "insert";
}
