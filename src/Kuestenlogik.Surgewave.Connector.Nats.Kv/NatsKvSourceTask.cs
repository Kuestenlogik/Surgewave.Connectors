using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv;

/// <summary>
/// Task that watches NATS KV store for changes and produces records.
/// </summary>
public sealed class NatsKvSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _natsUrl = NatsKvConnectorConfig.DefaultUrl;
    private string _bucket = "";
    private string _keyPattern = NatsKvConnectorConfig.DefaultKeyPattern;
    private string _watchMode = NatsKvConnectorConfig.DefaultWatchMode;
    private bool _includeHistory = NatsKvConnectorConfig.DefaultIncludeHistory;
    private bool _createBucket = NatsKvConnectorConfig.DefaultCreateBucketIfMissing;
    private int _pollIntervalMs = NatsKvConnectorConfig.DefaultPollIntervalMs;
    private string? _credentialsFile;
    private string? _token;
    private string? _username;
    private string? _password;

    private NatsConnection? _connection;
    private NatsJSContext? _jetStream;
    private INatsKVStore? _kvStore;
    private CancellationTokenSource? _watcherCts;
    private readonly Channel<NatsKVEntry<byte[]>> _entries = Channel.CreateBounded<NatsKVEntry<byte[]>>(1000);
    private Task? _watcherTask;
    private long _offset;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[NatsKvConnectorConfig.Topic];

        if (config.TryGetValue(NatsKvConnectorConfig.Url, out var url))
            _natsUrl = url;
        _bucket = config[NatsKvConnectorConfig.Bucket];

        if (config.TryGetValue(NatsKvConnectorConfig.KeyPattern, out var keyPattern))
            _keyPattern = keyPattern;
        if (config.TryGetValue(NatsKvConnectorConfig.WatchMode, out var watchMode))
            _watchMode = watchMode;
        if (config.TryGetValue(NatsKvConnectorConfig.IncludeHistory, out var includeHistory))
            _includeHistory = bool.Parse(includeHistory);
        if (config.TryGetValue(NatsKvConnectorConfig.CreateBucketIfMissing, out var createBucket))
            _createBucket = bool.Parse(createBucket);
        if (config.TryGetValue(NatsKvConnectorConfig.PollIntervalMs, out var pollInterval))
            _pollIntervalMs = int.Parse(pollInterval);
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
            _kvStore = await kvContext.CreateStoreAsync(_bucket);
        }
        else
        {
            _kvStore = await kvContext.GetStoreAsync(_bucket);
        }

        // Start the watcher
        _watcherCts = new CancellationTokenSource();
        StartWatcher();
    }

    private void StartWatcher()
    {
        _watcherTask = Task.Run(async () =>
        {
            try
            {
                var watchOpts = new NatsKVWatchOpts
                {
                    UpdatesOnly = _watchMode == NatsKvConnectorConfig.WatchUpdatesOnly,
                    IncludeHistory = _includeHistory
                };

                await foreach (var entry in _kvStore!.WatchAsync<byte[]>(_keyPattern, opts: watchOpts, cancellationToken: _watcherCts!.Token))
                {
                    await _entries.Writer.WriteAsync(entry, _watcherCts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
            finally
            {
                _entries.Writer.Complete();
            }
        });
    }

    public override void Stop()
    {
        _watcherCts?.Cancel();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _watcherCts?.Dispose();
            _watcherCts = null;
            try { _watcherTask?.GetAwaiter().GetResult(); } catch { /* ignore */ }
            try { _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* ignore */ }
            _connection = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Read available entries from the channel
        while (_entries.Reader.TryRead(out var entry))
        {
            var record = CreateRecord(entry);
            records.Add(record);

            // Limit batch size
            if (records.Count >= 100)
                break;
        }

        // If no records, wait a bit for new entries
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_pollIntervalMs);

                if (await _entries.Reader.WaitToReadAsync(cts.Token))
                {
                    while (_entries.Reader.TryRead(out var entry))
                    {
                        var record = CreateRecord(entry);
                        records.Add(record);

                        if (records.Count >= 100)
                            break;
                    }
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Poll timeout - normal
            }
        }

        return records;
    }

    private SourceRecord CreateRecord(NatsKVEntry<byte[]> entry)
    {
        var offset = Interlocked.Increment(ref _offset);

        var headers = new Dictionary<string, byte[]>
        {
            ["nats_kv_bucket"] = Encoding.UTF8.GetBytes(_bucket),
            ["nats_kv_key"] = Encoding.UTF8.GetBytes(entry.Key),
            ["nats_kv_revision"] = Encoding.UTF8.GetBytes(entry.Revision.ToString()),
            ["nats_kv_operation"] = Encoding.UTF8.GetBytes(entry.Operation.ToString())
        };

        // Serialize entry metadata to JSON for the value
        var entryData = new
        {
            bucket = _bucket,
            key = entry.Key,
            revision = entry.Revision,
            operation = entry.Operation.ToString(),
            created = entry.Created,
            delta = entry.Delta,
            value = entry.Value != null ? Convert.ToBase64String(entry.Value) : null
        };

        return new SourceRecord
        {
            Topic = _topic,
            Partition = 0,
            SourcePartition = new Dictionary<string, object>
            {
                ["bucket"] = _bucket
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["offset"] = offset,
                ["revision"] = entry.Revision
            },
            Key = Encoding.UTF8.GetBytes(entry.Key),
            Value = entry.Value ?? [],
            Headers = headers,
            Timestamp = entry.Created
        };
    }
}
