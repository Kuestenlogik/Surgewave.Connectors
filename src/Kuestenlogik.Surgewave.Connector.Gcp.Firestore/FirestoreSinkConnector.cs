using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

/// <summary>
/// Sink connector that writes records to Google Cloud Firestore.
/// Supports set, create, update, and merge write modes with batch operations.
/// </summary>
public sealed class FirestoreSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FirestoreSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(FirestoreConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High, "GCP project ID")
        .Define(FirestoreConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High, "GCP credentials JSON (alternative to file)")
        .Define(FirestoreConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High, "Path to GCP credentials file", EditorHint.FilePath)
        .Define(FirestoreConnectorConfig.EmulatorHostConfig, ConfigType.String, "", Importance.Low, "Firestore emulator host (for local testing)")
        .Define(FirestoreConnectorConfig.CollectionPathConfig, ConfigType.String, Importance.High, "Collection path to write to")
        .Define(FirestoreConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(FirestoreConnectorConfig.DocumentIdFieldConfig, ConfigType.String, "id", Importance.Medium, "Field to use as document ID")
        .Define(FirestoreConnectorConfig.WriteModeConfig, ConfigType.String, FirestoreConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: set, create, update, merge")
        .Define(FirestoreConnectorConfig.BatchSizeConfig, ConfigType.Int, FirestoreConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for bulk operations")
        .Define(FirestoreConnectorConfig.MaxRetryCountConfig, ConfigType.Int, FirestoreConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for transient failures")
        .Define(FirestoreConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)FirestoreConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in ms");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(FirestoreConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrEmpty(projectId))
            throw new ArgumentException($"Required configuration '{FirestoreConnectorConfig.ProjectIdConfig}' is missing");

        if (!config.TryGetValue(FirestoreConnectorConfig.CollectionPathConfig, out var collection) || string.IsNullOrEmpty(collection))
            throw new ArgumentException($"Required configuration '{FirestoreConnectorConfig.CollectionPathConfig}' is missing");

        if (!config.TryGetValue(FirestoreConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{FirestoreConnectorConfig.TopicsConfig}' is missing");

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
