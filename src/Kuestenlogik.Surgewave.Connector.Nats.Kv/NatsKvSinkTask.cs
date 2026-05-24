using System.Text;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv;

/// <summary>
/// Task that writes records to NATS KV store.
/// </summary>
public sealed class NatsKvSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _natsUrl = NatsKvConnectorConfig.DefaultUrl;
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

    private NatsConnection? _connection;
    private NatsJSContext? _jetStream;
    private INatsKVStore? _kvStore;

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(NatsKvConnectorConfig.Url, out var url))
            _natsUrl = url;
        _bucket = config[NatsKvConnectorConfig.Bucket];

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

        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        var opts = new NatsOpts
        {
            Url = _natsUrl
        };

        if (!string.IsNullOrEmpty(_credentialsFile))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = _credentialsFile } };
        }
        else if (!string.IsNullOrEmpty(_token))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Token = _token } };
        }
        else if (!string.IsNullOrEmpty(_username) && !string.IsNullOrEmpty(_password))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Username = _username, Password = _password } };
        }

        _connection = new NatsConnection(opts);
        await _connection.ConnectAsync();

        _jetStream = new NatsJSContext(_connection);
        var kvContext = new NatsKVContext(_jetStream);

        if (_createBucket)
        {
            var storeConfig = new NatsKVConfig(_bucket)
            {
                History = _history,
                MaxAge = _ttl > 0 ? TimeSpan.FromSeconds(_ttl) : TimeSpan.Zero
            };
            _kvStore = await kvContext.CreateStoreAsync(storeConfig);
        }
        else
        {
            _kvStore = await kvContext.GetStoreAsync(_bucket);
        }
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            try { _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* ignore */ }
            _connection = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _kvStore == null) return;

        foreach (var record in records)
        {
            try
            {
                var key = GetKey(record);
                if (string.IsNullOrEmpty(key)) continue;

                switch (_writeMode)
                {
                    case NatsKvConnectorConfig.WriteModeDelete:
                        await _kvStore.DeleteAsync(key, cancellationToken: cancellationToken);
                        break;

                    case NatsKvConnectorConfig.WriteModeCreate:
                        await _kvStore.CreateAsync(key, record.Value ?? [], cancellationToken: cancellationToken);
                        break;

                    case NatsKvConnectorConfig.WriteModeUpdate:
                        // Update requires the current revision, get it first
                        var entry = await _kvStore.GetEntryAsync<byte[]>(key, cancellationToken: cancellationToken);
                        await _kvStore.UpdateAsync(key, record.Value ?? [], entry.Revision, cancellationToken: cancellationToken);
                        break;

                    case NatsKvConnectorConfig.WriteModeUpsert:
                    default:
                        await _kvStore.PutAsync(key, record.Value ?? [], cancellationToken: cancellationToken);
                        break;
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    private string GetKey(SinkRecord record)
    {
        // First, try to get key from the record key
        if (record.Key != null && record.Key.Length > 0)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        // Try to get key from headers
        if (record.Headers?.TryGetValue("nats_kv_key", out var keyBytes) == true)
        {
            return Encoding.UTF8.GetString(keyBytes);
        }

        // Try to extract key from JSON value
        if (record.Value != null && record.Value.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                if (doc.RootElement.TryGetProperty(_keyField, out var keyElement))
                {
                    return keyElement.GetString() ?? "";
                }
            }
            catch
            {
                // Not valid JSON, ignore
            }
        }

        return "";
    }
}
