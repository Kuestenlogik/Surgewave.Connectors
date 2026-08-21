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
/// Task that writes objects to NATS JetStream Object Store.
/// </summary>
public sealed class NatsObjectStoreSinkTask : SinkTask
{
    private NatsConnection? _connection;
    private INatsObjStore? _objectStore;
    private string _bucketName = null!;
    private bool _createBucket;
    private string _objectNameField = null!;
    private string? _objectNamePrefix;
    private string? _contentType;
    private int _chunkSize;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _bucketName = config[NatsObjectStoreConnectorConfig.BucketName];
        var servers = config.TryGetValue(NatsObjectStoreConnectorConfig.Servers, out var srvs)
            ? srvs : NatsObjectStoreConnectorConfig.DefaultServer;
        _createBucket = (config.TryGetValue(NatsObjectStoreConnectorConfig.CreateBucket, out var createBkt) ? createBkt : "true") == "true";
        _objectNameField = config.TryGetValue(NatsObjectStoreConnectorConfig.ObjectNameField, out var objNameField) ? objNameField : "name";
        _objectNamePrefix = config.TryGetValue(NatsObjectStoreConnectorConfig.ObjectNamePrefix, out var objNamePrefix) ? objNamePrefix : null;
        _contentType = config.TryGetValue(NatsObjectStoreConnectorConfig.ContentType, out var contentType) ? contentType : null;
        _chunkSize = config.TryGetValue(NatsObjectStoreConnectorConfig.ChunkSize, out var chunkSize) && !string.IsNullOrWhiteSpace(chunkSize)
            ? int.Parse(chunkSize, CultureInfo.InvariantCulture)
            : NatsObjectStoreConnectorConfig.DefaultChunkSize;

        // Build connection options
        var opts = new NatsOpts
        {
            Url = servers
        };

        var username = config.TryGetValue(NatsObjectStoreConnectorConfig.Username, out var usr) ? usr : null;
        var password = config.TryGetValue(NatsObjectStoreConnectorConfig.Password, out var pwd) ? pwd : null;
        var token = config.TryGetValue(NatsObjectStoreConnectorConfig.Token, out var tkn) ? tkn : null;

        if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
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
    /// Connects and resolves the bucket on first use. Failures propagate to the caller
    /// so the batch is retried instead of being dropped, and the next put retries setup.
    /// </summary>
    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_objectStore != null) return;

        await _connection!.ConnectAsync();

        var js = new NatsJSContext(_connection);
        var objContext = new NatsObjContext(js);

        _objectStore = _createBucket
            ? await objContext.CreateObjectStoreAsync(new NatsObjConfig(_bucketName), cancellationToken)
            : await objContext.GetObjectStoreAsync(_bucketName, cancellationToken);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        try
        {
            await EnsureInitializedAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            // Connection/bucket setup failed: fail the batch instead of discarding it
            // while the worker advances the consumer offsets past these records.
            Context?.RaiseError?.Invoke(ex);
            throw;
        }

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                string objectName;
                byte[] content;
                string? description = null;

                // Try to parse as JSON to extract name and content
                try
                {
                    using var doc = JsonDocument.Parse(record.Value);
                    var root = doc.RootElement;

                    // Get object name
                    if (root.TryGetProperty(_objectNameField, out var nameProp))
                    {
                        objectName = nameProp.GetString() ?? Guid.NewGuid().ToString();
                    }
                    else if (record.Key != null)
                    {
                        objectName = Encoding.UTF8.GetString(record.Key);
                    }
                    else
                    {
                        objectName = Guid.NewGuid().ToString();
                    }

                    // Get content
                    if (root.TryGetProperty("content", out var contentProp))
                    {
                        var contentStr = contentProp.GetString();
                        if (root.TryGetProperty("contentEncoding", out var encProp) &&
                            encProp.GetString()?.ToLowerInvariant() == "base64")
                        {
                            content = Convert.FromBase64String(contentStr ?? "");
                        }
                        else
                        {
                            content = Encoding.UTF8.GetBytes(contentStr ?? "");
                        }
                    }
                    else if (root.TryGetProperty("data", out var dataProp))
                    {
                        var dataStr = dataProp.GetString();
                        content = Convert.FromBase64String(dataStr ?? "");
                    }
                    else
                    {
                        // Use entire payload as content
                        content = record.Value;
                    }

                    // Get optional description
                    if (root.TryGetProperty("description", out var descProp))
                    {
                        description = descProp.GetString();
                    }
                }
                catch
                {
                    // Not JSON, use key as name and value as content
                    objectName = record.Key != null
                        ? Encoding.UTF8.GetString(record.Key)
                        : Guid.NewGuid().ToString();
                    content = record.Value;
                }

                // Apply prefix
                if (!string.IsNullOrEmpty(_objectNamePrefix))
                {
                    objectName = _objectNamePrefix + objectName;
                }

                // Check for delete operation
                if (record.Headers?.TryGetValue("nats.objectstore.operation", out var opBytes) == true &&
                    Encoding.UTF8.GetString(opBytes) == "delete")
                {
                    await _objectStore!.DeleteAsync(objectName, cancellationToken);
                }
                else
                {
                    // Put object
                    using var stream = new MemoryStream(content);
                    var putOpts = new ObjectMetadata
                    {
                        Name = objectName,
                        Description = description
                    };

                    if (!string.IsNullOrEmpty(_contentType))
                    {
                        putOpts.Headers = new Dictionary<string, string[]> { ["Content-Type"] = [_contentType] };
                    }

                    if (_chunkSize > 0)
                    {
                        putOpts.Options = new MetaDataOptions { MaxChunkSize = _chunkSize };
                    }

                    await _objectStore!.PutAsync(putOpts, stream, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                // Never swallow a failed object write: the worker would commit offsets
                // for records that never reached the bucket.
                Context?.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection?.DisposeAsync().AsTask().Wait();
        }
        base.Dispose(disposing);
    }
}
