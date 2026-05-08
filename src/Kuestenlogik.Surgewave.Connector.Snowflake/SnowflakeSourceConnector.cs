using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Snowflake;

/// <summary>
/// Source connector that captures data from Snowflake tables, queries, or CDC streams.
/// Supports table polling, custom queries, and Snowflake Streams for real-time CDC.
/// </summary>
public sealed class SnowflakeSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SnowflakeSourceTask);

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
        .Define(SnowflakeConnectorConfig.ModeConfig, ConfigType.String, SnowflakeConnectorConfig.DefaultMode, Importance.Medium, "Mode: table, query, stream", EditorHint.Select, options: ["table", "query", "stream"])
        .Define(SnowflakeConnectorConfig.TableConfig, ConfigType.String, "", Importance.High, "Table name (for table/stream mode)")
        .Define(SnowflakeConnectorConfig.QueryConfig, ConfigType.String, "", Importance.Medium, "Custom SQL query (for query mode)", EditorHint.Code, "sql")
        .Define(SnowflakeConnectorConfig.StreamNameConfig, ConfigType.String, "", Importance.Medium, "Stream name (for stream mode, auto-created if not exists)")
        .Define(SnowflakeConnectorConfig.TopicPatternConfig, ConfigType.String, SnowflakeConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern")
        .Define(SnowflakeConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)SnowflakeConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(SnowflakeConnectorConfig.MaxRowsPerPollConfig, ConfigType.Int, SnowflakeConnectorConfig.DefaultMaxRowsPerPoll, Importance.Low, "Max rows per poll")
        .Define(SnowflakeConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Snowflake metadata in output")
        .Define(SnowflakeConnectorConfig.TimestampColumnConfig, ConfigType.String, "", Importance.Low, "Timestamp column for incremental polling")
        .Define(SnowflakeConnectorConfig.IncrementingColumnConfig, ConfigType.String, "", Importance.Low, "Incrementing column for incremental polling");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(SnowflakeConnectorConfig.AccountConfig, out var account) || string.IsNullOrEmpty(account))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.AccountConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.UserConfig, out var user) || string.IsNullOrEmpty(user))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.UserConfig}' is missing");

        if (!config.TryGetValue(SnowflakeConnectorConfig.DatabaseConfig, out var database) || string.IsNullOrEmpty(database))
            throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.DatabaseConfig}' is missing");

        var mode = GetConfigValue(config, SnowflakeConnectorConfig.ModeConfig, SnowflakeConnectorConfig.DefaultMode);

        // Validate mode-specific requirements
        if (mode.Equals("table", StringComparison.OrdinalIgnoreCase) || mode.Equals("stream", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(SnowflakeConnectorConfig.TableConfig, out var table) || string.IsNullOrEmpty(table))
                throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.TableConfig}' is missing for {mode} mode");
        }
        else if (mode.Equals("query", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.TryGetValue(SnowflakeConnectorConfig.QueryConfig, out var query) || string.IsNullOrEmpty(query))
                throw new ArgumentException($"Required configuration '{SnowflakeConnectorConfig.QueryConfig}' is missing for query mode");
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
        // Single task - Snowflake handles query execution internally
        return [new Dictionary<string, string>(_config)];
    }
}
