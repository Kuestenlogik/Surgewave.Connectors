using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using NATS.Client.ObjectStore.Models;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore;

/// <summary>
/// Task that watches NATS JetStream Object Store for changes.
/// </summary>
public sealed class NatsObjectStoreSourceTask : SourceTask
{
    private NatsConnection? _connection;
    private INatsObjStore? _objectStore;
    private string _topic = null!;
    private string _bucketName = null!;
    private bool _includeHistory;
    private string? _watchPrefix;
    private bool _includeDeletes;
    private bool _includeContent;
    private int _maxContentSize;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private CancellationTokenSource? _watchCts;
    private Task? _watchTask;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[NatsObjectStoreConnectorConfig.Topic];
        _bucketName = config[NatsObjectStoreConnectorConfig.BucketName];
        var servers = config.TryGetValue(NatsObjectStoreConnectorConfig.Servers, out var srvs)
            ? srvs : NatsObjectStoreConnectorConfig.DefaultServer;
        _watchPrefix = config.TryGetValue(NatsObjectStoreConnectorConfig.WatchPrefix, out var watchPrefix) ? watchPrefix : null;
        _includeHistory = (config.TryGetValue(NatsObjectStoreConnectorConfig.IncludeHistory, out var includeHist) ? includeHist : "false") == "true";
        _includeDeletes = (config.TryGetValue(NatsObjectStoreConnectorConfig.IncludeDeletes, out var includeDeletes) ? includeDeletes : "true") == "true";
        _includeContent = (config.TryGetValue(NatsObjectStoreConnectorConfig.IncludeContent, out var includeContent) ? includeContent : "true") == "true";
        _maxContentSize = config.TryGetValue(NatsObjectStoreConnectorConfig.MaxContentSize, out var maxContentSize) && !string.IsNullOrWhiteSpace(maxContentSize)
            ? int.Parse(maxContentSize, CultureInfo.InvariantCulture)
            : NatsObjectStoreConnectorConfig.DefaultMaxContentSize;

        // Build connection options
        var opts = new NatsOpts
        {
            Url = servers
        };

        var username = config.TryGetValue(NatsObjectStoreConnectorConfig.Username, out var usr) ? usr : null;
        var password = config.TryGetValue(NatsObjectStoreConnectorConfig.Password, out var pwd) ? pwd : null;
        var token = config.TryGetValue(NatsObjectStoreConnectorConfig.Token, out var tkn) ? tkn : null;
        var credentialsFile = config.TryGetValue(NatsObjectStoreConnectorConfig.CredentialsFile, out var creds) ? creds : null;

        if (!string.IsNullOrWhiteSpace(credentialsFile))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = credentialsFile } };
        }
        else if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
        {
            opts = opts with { AuthOpts = new NatsAuthOpts { Username = username, Password = password } };
        }
        else if (!string.IsNullOrWhiteSpace(token))
        {
            opts = opts with { AuthOpts = new NatsAuthOpts { Token = token } };
        }

        _connection = new NatsConnection(opts);
    }

    /// <summary>
    /// Connects, resolves the bucket and starts the watch on first poll. A watch that
    /// ended (error, dropped subscription) is restarted on the next poll; failures
    /// propagate so the caller can surface them instead of polling empty forever.
    /// </summary>
    private async Task EnsureWatchingAsync(CancellationToken cancellationToken)
    {
        if (_watchTask is { IsCompleted: false }) return;

        _watchCts?.Cancel();
        _watchCts?.Dispose();
        _watchCts = null;
        _watchTask = null;

        if (_objectStore == null)
        {
            await _connection!.ConnectAsync();

            var js = new NatsJSContext(_connection);
            var objContext = new NatsObjContext(js);

            _objectStore = await objContext.GetObjectStoreAsync(_bucketName, cancellationToken);
        }

        var watchOpts = new NatsObjWatchOpts
        {
            IgnoreDeletes = !_includeDeletes,
            IncludeHistory = _includeHistory
        };

        _watchCts = new CancellationTokenSource();
        _watchTask = WatchAsync(watchOpts, _watchCts.Token);
    }

    private async Task WatchAsync(NatsObjWatchOpts opts, CancellationToken ct)
    {
        try
        {
            await foreach (var info in _objectStore!.WatchAsync(opts, ct))
            {
                try
                {
                    // Filter by prefix if specified
                    if (!string.IsNullOrEmpty(_watchPrefix) &&
                        !info.Name.StartsWith(_watchPrefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var record = await CreateRecordAsync(info, ct);
                    _pendingRecords.Enqueue(record);
                }
                catch (Exception ex)
                {
                    // One object could not be turned into a record - keep watching, but
                    // make the failure visible instead of dropping it silently.
                    Context?.RaiseError?.Invoke(ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            // The watch subscription died; PollAsync restarts it on the next call.
            Context?.RaiseError?.Invoke(ex);
        }
    }

    private async Task<SourceRecord> CreateRecordAsync(ObjectMetadata info, CancellationToken ct)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        byte[]? content = null;
        if (_includeContent && !info.Deleted && info.Size <= (ulong)_maxContentSize)
        {
            try
            {
                using var ms = new MemoryStream();
                await _objectStore!.GetAsync(info.Name, ms, cancellationToken: ct);
                content = ms.ToArray();
            }
            catch
            {
                // Object may have been deleted, skip content
            }
        }

        var payload = new
        {
            type = info.Deleted ? "delete" : "put",
            name = info.Name,
            bucket = info.Bucket,
            size = info.Size,
            chunks = info.Chunks,
            digest = info.Digest,
            deleted = info.Deleted,
            modified = info.MTime,
            description = info.Description,
            headers = info.Headers,
            content = content != null ? Convert.ToBase64String(content) : null,
            contentEncoding = content != null ? "base64" : null,
            timestamp = DateTime.UtcNow
        };

        var bucket = info.Bucket ?? "unknown";
        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "nats-objectstore",
                ["bucket"] = bucket
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["name"] = info.Name,
                ["nuid"] = info.Nuid ?? ""
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(info.Name),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["nats.objectstore.bucket"] = Encoding.UTF8.GetBytes(bucket),
                ["nats.objectstore.name"] = Encoding.UTF8.GetBytes(info.Name),
                ["nats.objectstore.operation"] = Encoding.UTF8.GetBytes(info.Deleted ? "delete" : "put"),
                ["nats.objectstore.size"] = Encoding.UTF8.GetBytes(info.Size.ToString(CultureInfo.InvariantCulture))
            }
        };
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        try
        {
            await EnsureWatchingAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            // Connect/bucket resolution failed. Surface and rethrow so a broken bucket is
            // distinguishable from an idle one instead of polling empty forever; the
            // worker backs off and the next poll retries the setup.
            Context?.RaiseError?.Invoke(ex);
            throw;
        }

        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
            if (records.Count >= 100) break;
        }

        return records;
    }

    public override void Stop()
    {
        _watchCts?.Cancel();
        try { _watchTask?.Wait(TimeSpan.FromSeconds(5)); } catch { }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _watchCts?.Dispose();
            _connection?.DisposeAsync().AsTask().Wait();
        }
        base.Dispose(disposing);
    }
}
