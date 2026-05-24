using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

/// <summary>
/// Source connector that captures changes from Google Cloud Firestore.
/// Supports both polling and real-time listener modes for CDC-compatible output.
/// </summary>
public sealed class FirestoreSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FirestoreSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(FirestoreConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High, "GCP project ID")
        .Define(FirestoreConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High, "GCP credentials JSON (alternative to file)")
        .Define(FirestoreConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High, "Path to GCP credentials file", EditorHint.FilePath)
        .Define(FirestoreConnectorConfig.EmulatorHostConfig, ConfigType.String, "", Importance.Low, "Firestore emulator host (for local testing)")
        .Define(FirestoreConnectorConfig.CollectionPathConfig, ConfigType.String, Importance.High, "Collection path to monitor")
        .Define(FirestoreConnectorConfig.TopicPatternConfig, ConfigType.String, FirestoreConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${collection})")
        .Define(FirestoreConnectorConfig.WatchModeConfig, ConfigType.String, FirestoreConnectorConfig.DefaultWatchMode, Importance.Medium, "Watch mode: poll, listen")
        .Define(FirestoreConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)FirestoreConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms (poll mode)")
        .Define(FirestoreConnectorConfig.MaxDocumentsPerPollConfig, ConfigType.Int, FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll, Importance.Low, "Max documents per poll")
        .Define(FirestoreConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Firestore metadata in output")
        .Define(FirestoreConnectorConfig.QueryFilterConfig, ConfigType.String, "", Importance.Low, "Query filter (field:op:value)", EditorHint.Code, "odata")
        .Define(FirestoreConnectorConfig.OrderByFieldConfig, ConfigType.String, "", Importance.Low, "Field to order by")
        .Define(FirestoreConnectorConfig.OrderDirectionConfig, ConfigType.String, FirestoreConnectorConfig.DefaultOrderDirection, Importance.Low, "Order direction: asc, desc")
        .Define(FirestoreConnectorConfig.TimestampFieldConfig, ConfigType.String, "", Importance.Low, "Timestamp field for incremental polling");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(FirestoreConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrEmpty(projectId))
            throw new ArgumentException($"Required configuration '{FirestoreConnectorConfig.ProjectIdConfig}' is missing");

        if (!config.TryGetValue(FirestoreConnectorConfig.CollectionPathConfig, out var collection) || string.IsNullOrEmpty(collection))
            throw new ArgumentException($"Required configuration '{FirestoreConnectorConfig.CollectionPathConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Firestore handles distribution internally
        return [new Dictionary<string, string>(_config)];
    }
}
