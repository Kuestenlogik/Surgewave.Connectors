using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

/// <summary>
/// Sink connector that writes records to DynamoDB tables.
/// Supports PutItem, UpdateItem, DeleteItem, and BatchWriteItem operations.
/// </summary>
public sealed class DynamoDbSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(DynamoDbSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(DynamoDbConnectorConfig.TableNameConfig, ConfigType.String, Importance.High, "DynamoDB table name")
        .Define(DynamoDbConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(DynamoDbConnectorConfig.RegionConfig, ConfigType.String, DynamoDbConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(DynamoDbConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(DynamoDbConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(DynamoDbConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(DynamoDbConnectorConfig.WriteModeConfig, ConfigType.String, DynamoDbConnectorConfig.WriteModePut, Importance.Medium, "Write mode (put, insert, update, delete)", EditorHint.Select, options: ["put", "insert", "update", "delete"])
        .Define(DynamoDbConnectorConfig.PartitionKeyFieldConfig, ConfigType.String, Importance.High, "Field to use as partition key")
        .Define(DynamoDbConnectorConfig.SortKeyFieldConfig, ConfigType.String, "", Importance.Medium, "Field to use as sort key (optional)")
        .Define(DynamoDbConnectorConfig.BatchSizeConfig, ConfigType.Int, DynamoDbConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for writes (max 25)")
        .Define(DynamoDbConnectorConfig.AutoCreateTableConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create table if not exists")
        .Define(DynamoDbConnectorConfig.BillingModeConfig, ConfigType.String, DynamoDbConnectorConfig.BillingModePayPerRequest, Importance.Low, "Billing mode for auto-created table", EditorHint.Select, options: ["PAY_PER_REQUEST", "PROVISIONED"])
        .Define(DynamoDbConnectorConfig.ReadCapacityConfig, ConfigType.Int, (int)DynamoDbConnectorConfig.DefaultReadCapacity, Importance.Low, "Read capacity units (for PROVISIONED mode)")
        .Define(DynamoDbConnectorConfig.WriteCapacityConfig, ConfigType.Int, (int)DynamoDbConnectorConfig.DefaultWriteCapacity, Importance.Low, "Write capacity units (for PROVISIONED mode)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(DynamoDbConnectorConfig.TableNameConfig, out var tableName) || string.IsNullOrEmpty(tableName))
            throw new ArgumentException($"Required configuration '{DynamoDbConnectorConfig.TableNameConfig}' is missing");

        if (!config.TryGetValue(DynamoDbConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{DynamoDbConnectorConfig.TopicsConfig}' is missing");

        if (!config.TryGetValue(DynamoDbConnectorConfig.PartitionKeyFieldConfig, out var pkField) || string.IsNullOrEmpty(pkField))
            throw new ArgumentException($"Required configuration '{DynamoDbConnectorConfig.PartitionKeyFieldConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

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
