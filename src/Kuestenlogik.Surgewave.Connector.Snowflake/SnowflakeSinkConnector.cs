using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Snowflake;

/// <summary>
/// Sink connector that writes records to Snowflake tables.
/// Supports INSERT, UPSERT, and MERGE operations with batch processing.
/// </summary>
public sealed class SnowflakeSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SnowflakeSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(SnowflakeConnectorConfig.AccountConfig, ConfigType.String, Importance.High, "Snowflake account identifier")
        .Define(SnowflakeConnectorConfig.UserConfig, ConfigType.String, Importance.High, "Snowflake username")
        .Define(SnowflakeConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.High, "Snowflake password")
        .Define(SnowflakeConnectorConfig.DatabaseConfig, ConfigType.String, Importance.High, "Database name")
        .Define(SnowflakeConnectorConfig.SchemaConfig, ConfigType.String, "PUBLIC", Importance.High, "Schema name")
        .Define(SnowflakeConnectorConfig.WarehouseConfig, ConfigType.String, "", Importance.Medium, "Warehouse name")
        .Define(SnowflakeConnectorConfig.RoleConfig, ConfigType.String, "", Importance.Medium, "Role name")
        .Define(SnowflakeConnectorConfig.AuthenticatorConfig, ConfigType.String, "snowflake", Importance.Low, "Authenticator: snowflake, externalbrowser, oauth, jwt", EditorHint.Select, options: ["snowflake", "externalbrowser", "oauth", "jwt"])
        .Define(SnowflakeConnectorConfig.PrivateKeyFileConfig, ConfigType.String, "", Importance.Low, "Path to private key file (for key-pair auth)", EditorHint.FilePath)
        .Define(SnowflakeConnectorConfig.PrivateKeyPassphraseConfig, ConfigType.Password, "", Importance.Low, "Private key passphrase")
        .Define(SnowflakeConnectorConfig.OAuthTokenConfig, ConfigType.Password, "", Importance.Low, "OAuth access token")
        .Define(SnowflakeConnectorConfig.TableConfig, ConfigType.String, Importance.High, "Target table name")
        .Define(SnowflakeConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(SnowflakeConnectorConfig.WriteModeConfig, ConfigType.String, SnowflakeConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: insert, upsert, merge", EditorHint.Select, options: ["insert", "upsert", "merge"])
        .Define(SnowflakeConnectorConfig.BatchSizeConfig, ConfigType.Int, SnowflakeConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for bulk operations")
        .Define(SnowflakeConnectorConfig.StageNameConfig, ConfigType.String, "", Importance.Low, "Internal stage name for bulk loading")
        .Define(SnowflakeConnectorConfig.UseSnowpipeConfig, ConfigType.Boolean, false, Importance.Low, "Use Snowpipe for continuous loading")
        .Define(SnowflakeConnectorConfig.PipeNameConfig, ConfigType.String, "", Importance.Low, "Snowpipe name")
        .Define(SnowflakeConnectorConfig.KeyColumnsConfig, ConfigType.String, "", Importance.Medium, "Key columns for upsert/merge (comma-separated)")
        .Define(SnowflakeConnectorConfig.MaxRetryCountConfig, ConfigType.Int, SnowflakeConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for transient failures")
        .Define(SnowflakeConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)SnowflakeConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in milliseconds")
        .Define(SnowflakeConnectorConfig.AutoCreateTableConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create table if not exists");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(SnowflakeConnectorConfig.AccountConfig, out var account) || string.IsNullOrEmpty(account))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.AccountConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.UserConfig, out var user) || string.IsNullOrEmpty(user))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.UserConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.DatabaseConfig, out var database) || string.IsNullOrEmpty(database))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.DatabaseConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.TableConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.TopicsConfig}' is missing");

        // Validate upsert/merge mode has key columns
        var writeMode = GetConfigValue(config, SnowflakeConnectorConfig.WriteModeConfig, SnowflakeConnectorConfig.DefaultWriteMode);
        if (!writeMode.Equals("insert", StringComparison.OrdinalIgnoreCase))
        {
            var keyColumns = GetConfigValue(config, SnowflakeConnectorConfig.KeyColumnsConfig, "");
            if (string.IsNullOrEmpty(keyColumns))
                throw new ArgumentException($"'{SnowflakeConnectorConfig.KeyColumnsConfig}' is required for {writeMode} mode");
        }

        _config = new Dictionary<string, string>(config);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity
        return [new Dictionary<string, string>(_config)];
    }
}
