using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv;

/// <summary>
/// Sink connector that writes records to NATS Key-Value store.
/// </summary>
public sealed class NatsKvSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(NatsKvSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(NatsKvConnectorConfig.Topics, ConfigType.String, Importance.High, "Surgewave topics to consume from", EditorHint.Topic)
        .Define(NatsKvConnectorConfig.Url, ConfigType.String, NatsKvConnectorConfig.DefaultUrl, Importance.High, "NATS server URL")
        .Define(NatsKvConnectorConfig.Bucket, ConfigType.String, Importance.High, "KV bucket name")
        .Define(NatsKvConnectorConfig.KeyField, ConfigType.String, NatsKvConnectorConfig.DefaultKeyField, Importance.Medium, "Field name to use as key (from record key or JSON field)")
        .Define(NatsKvConnectorConfig.WriteMode, ConfigType.String, NatsKvConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: put (upsert), create, update, delete", EditorHint.Select, options: ["put", "create", "update", "delete"])
        .Define(NatsKvConnectorConfig.CreateBucketIfMissing, ConfigType.Boolean, NatsKvConnectorConfig.DefaultCreateBucketIfMissing, Importance.Low, "Create bucket if it doesn't exist")
        .Define(NatsKvConnectorConfig.History, ConfigType.Int, NatsKvConnectorConfig.DefaultHistory, Importance.Low, "Number of history entries to keep")
        .Define(NatsKvConnectorConfig.Ttl, ConfigType.Int, NatsKvConnectorConfig.DefaultTtl, Importance.Low, "TTL in seconds (0 = no TTL)")
        .Define(NatsKvConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.Medium, "NATS credentials file path", EditorHint.FilePath)
        .Define(NatsKvConnectorConfig.Token, ConfigType.Password, "", Importance.Medium, "NATS authentication token")
        .Define(NatsKvConnectorConfig.Username, ConfigType.String, "", Importance.Medium, "NATS username")
        .Define(NatsKvConnectorConfig.Password, ConfigType.Password, "", Importance.Medium, "NATS password");

    private string _topics = "";
    private string _url = NatsKvConnectorConfig.DefaultUrl;
    private string _bucket = "";
    private string _keyField = NatsKvConnectorConfig.DefaultKeyField;
    private string _writeMode = NatsKvConnectorConfig.DefaultWriteMode;
    private bool _createBucket = NatsKvConnectorConfig.DefaultCreateBucketIfMissing;
    private int _history = NatsKvConnectorConfig.DefaultHistory;
    private int _ttl = NatsKvConnectorConfig.DefaultTtl;
    private string? _credentialsFile;
    private string? _token;
    private string? _username;
    private string? _password;

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(NatsKvConnectorConfig.Topics, out _topics!) || string.IsNullOrEmpty(_topics))
            throw new ArgumentException($"Missing required config: {NatsKvConnectorConfig.Topics}");

        if (!config.TryGetValue(NatsKvConnectorConfig.Bucket, out _bucket!) || string.IsNullOrEmpty(_bucket))
            throw new ArgumentException($"Missing required config: {NatsKvConnectorConfig.Bucket}");

        if (config.TryGetValue(NatsKvConnectorConfig.Url, out var url))
            _url = url;
        if (config.TryGetValue(NatsKvConnectorConfig.KeyField, out var keyField))
            _keyField = keyField;
        if (config.TryGetValue(NatsKvConnectorConfig.WriteMode, out var writeMode))
            _writeMode = writeMode;
        if (config.TryGetValue(NatsKvConnectorConfig.CreateBucketIfMissing, out var createBucket))
            _createBucket = bool.Parse(createBucket);
        if (config.TryGetValue(NatsKvConnectorConfig.History, out var history))
            _history = int.Parse(history);
        if (config.TryGetValue(NatsKvConnectorConfig.Ttl, out var ttl))
            _ttl = int.Parse(ttl);
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
            [NatsKvConnectorConfig.Topics] = _topics,
            [NatsKvConnectorConfig.Url] = _url,
            [NatsKvConnectorConfig.Bucket] = _bucket,
            [NatsKvConnectorConfig.KeyField] = _keyField,
            [NatsKvConnectorConfig.WriteMode] = _writeMode,
            [NatsKvConnectorConfig.CreateBucketIfMissing] = _createBucket.ToString(),
            [NatsKvConnectorConfig.History] = _history.ToString(),
            [NatsKvConnectorConfig.Ttl] = _ttl.ToString(),
            [NatsKvConnectorConfig.CredentialsFile] = _credentialsFile ?? "",
            [NatsKvConnectorConfig.Token] = _token ?? "",
            [NatsKvConnectorConfig.Username] = _username ?? "",
            [NatsKvConnectorConfig.Password] = _password ?? ""
        }];
    }
}
