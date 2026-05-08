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
    private string _objectNameField = null!;
    private string? _objectNamePrefix;
    private string? _contentType;
    private bool _isInitialized;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var bucketName = config[NatsObjectStoreConnectorConfig.BucketName];
        var servers = config.TryGetValue(NatsObjectStoreConnectorConfig.Servers, out var srvs)
            ? srvs : NatsObjectStoreConnectorConfig.DefaultServer;
        var createBucket = (config.TryGetValue(NatsObjectStoreConnectorConfig.CreateBucket, out var createBkt) ? createBkt : "true") == "true";
        _objectNameField = config.TryGetValue(NatsObjectStoreConnectorConfig.ObjectNameField, out var objNameField) ? objNameField : "name";
        _objectNamePrefix = config.TryGetValue(NatsObjectStoreConnectorConfig.ObjectNamePrefix, out var objNamePrefix) ? objNamePrefix : null;
        _contentType = config.TryGetValue(NatsObjectStoreConnectorConfig.ContentType, out var contentType) ? contentType : null;

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

        // Initialize asynchronously
        _ = InitializeAsync(bucketName, createBucket);
    }

    private async Task InitializeAsync(string bucketName, bool createBucket)
    {
        try
        {
            await _connection!.ConnectAsync();

            var js = new NatsJSContext(_connection);
            var objContext = new NatsObjContext(js);

            if (createBucket)
            {
                _objectStore = await objContext.CreateObjectStoreAsync(new NatsObjConfig(bucketName));
            }
            else
            {
                _objectStore = await objContext.GetObjectStoreAsync(bucketName);
            }

            _isInitialized = true;
        }
        catch (Exception)
        {
            // Log and continue - will retry on put
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Wait for initialization
        var retries = 0;
        while (!_isInitialized && retries < 50)
        {
            await Task.Delay(100, cancellationToken);
            retries++;
        }

        if (!_isInitialized) return;

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

                    await _objectStore!.PutAsync(putOpts, stream, cancellationToken: cancellationToken);
                }
            }
            catch (Exception)
            {
                // Log and continue
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
