using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv;

/// <summary>
/// Source connector that watches NATS Key-Value store for changes.
/// </summary>
public sealed class NatsKvSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(NatsKvSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsKvConnectorConfig.Topic, ConfigType.String, Importance.High, "Target Surgewave topic for produced records", EditorHint.Topic)
        .Define(NatsKvConnectorConfig.Url, ConfigType.String, NatsKvConnectorConfig.DefaultUrl, Importance.High, "NATS server URL")
        .Define(NatsKvConnectorConfig.Bucket, ConfigType.String, Importance.High, "KV bucket name")
        .Define(NatsKvConnectorConfig.KeyPattern, ConfigType.String, NatsKvConnectorConfig.DefaultKeyPattern, Importance.Medium, "Key pattern to watch (supports wildcards)")
        .Define(NatsKvConnectorConfig.WatchMode, ConfigType.String, NatsKvConnectorConfig.DefaultWatchMode, Importance.Medium, "Watch mode: all (initial values + updates) or updates", EditorHint.Select, options: ["all", "updates"])
        .Define(NatsKvConnectorConfig.IncludeHistory, ConfigType.Boolean, NatsKvConnectorConfig.DefaultIncludeHistory, Importance.Low, "Include history entries")
        .Define(NatsKvConnectorConfig.CreateBucketIfMissing, ConfigType.Boolean, NatsKvConnectorConfig.DefaultCreateBucketIfMissing, Importance.Low, "Create bucket if it doesn't exist")
        .Define(NatsKvConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.Medium, "NATS credentials file path", EditorHint.FilePath)
        .Define(NatsKvConnectorConfig.Token, ConfigType.Password, "", Importance.Medium, "NATS authentication token")
        .Define(NatsKvConnectorConfig.Username, ConfigType.String, "", Importance.Medium, "NATS username")
        .Define(NatsKvConnectorConfig.Password, ConfigType.Password, "", Importance.Medium, "NATS password");

    private string _topic = "";
    private string _url = NatsKvConnectorConfig.DefaultUrl;
    private string _bucket = "";
    private string _keyPattern = NatsKvConnectorConfig.DefaultKeyPattern;
    private string _watchMode = NatsKvConnectorConfig.DefaultWatchMode;
    private bool _includeHistory = NatsKvConnectorConfig.DefaultIncludeHistory;
    private bool _createBucket = NatsKvConnectorConfig.DefaultCreateBucketIfMissing;
    private string? _credentialsFile;
    private string? _token;
    private string? _username;
    private string? _password;

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(NatsKvConnectorConfig.Topic, out _topic!) || string.IsNullOrEmpty(_topic))
            throw new ArgumentException($"Missing required config: {NatsKvConnectorConfig.Topic}");

        if (!config.TryGetValue(NatsKvConnectorConfig.Bucket, out _bucket!) || string.IsNullOrEmpty(_bucket))
            throw new ArgumentException($"Missing required config: {NatsKvConnectorConfig.Bucket}");

        if (config.TryGetValue(NatsKvConnectorConfig.Url, out var url))
            _url = url;
        if (config.TryGetValue(NatsKvConnectorConfig.KeyPattern, out var keyPattern))
            _keyPattern = keyPattern;
        if (config.TryGetValue(NatsKvConnectorConfig.WatchMode, out var watchMode))
            _watchMode = watchMode;
        if (config.TryGetValue(NatsKvConnectorConfig.IncludeHistory, out var includeHistory))
            _includeHistory = bool.Parse(includeHistory);
        if (config.TryGetValue(NatsKvConnectorConfig.CreateBucketIfMissing, out var createBucket))
            _createBucket = bool.Parse(createBucket);
        config.TryGetValue(NatsKvConnectorConfig.CredentialsFile, out _credentialsFile);
        config.TryGetValue(NatsKvConnectorConfig.Token, out _token);
        config.TryGetValue(NatsKvConnectorConfig.Username, out _username);
        config.TryGetValue(NatsKvConnectorConfig.Password, out _password);
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topic] = _topic,
            [NatsKvConnectorConfig.Url] = _url,
            [NatsKvConnectorConfig.Bucket] = _bucket,
            [NatsKvConnectorConfig.KeyPattern] = _keyPattern,
            [NatsKvConnectorConfig.WatchMode] = _watchMode,
            [NatsKvConnectorConfig.IncludeHistory] = _includeHistory.ToString(),
            [NatsKvConnectorConfig.CreateBucketIfMissing] = _createBucket.ToString(),
            [NatsKvConnectorConfig.CredentialsFile] = _credentialsFile ?? "",
            [NatsKvConnectorConfig.Token] = _token ?? "",
            [NatsKvConnectorConfig.Username] = _username ?? "",
            [NatsKvConnectorConfig.Password] = _password ?? ""
        }];
    }
}
