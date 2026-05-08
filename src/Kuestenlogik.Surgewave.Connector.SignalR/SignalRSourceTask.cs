using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;
using Microsoft.AspNetCore.SignalR.Client;

namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// SignalR Source Task - Receives messages from a SignalR hub and produces them as SourceRecords.
/// </summary>
public sealed class SignalRSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private HubConnection? _connection;
    private string _topic = "";
    private string _method = SignalRConfig.DefaultMethod;
    private readonly Channel<SourceRecord> _recordChannel = Channel.CreateBounded<SourceRecord>(
        new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    private IDisposable? _subscription;
    private long _messageCount;

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(SignalRConfig.HubUrl, out var hubUrl) || string.IsNullOrEmpty(hubUrl))
        {
            throw new ArgumentException($"Missing required config: {SignalRConfig.HubUrl}");
        }

        if (!config.TryGetValue(SignalRConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {SignalRConfig.Topic}");
        }

        _topic = topic;

        if (config.TryGetValue(SignalRConfig.Method, out var method) && !string.IsNullOrEmpty(method))
        {
            _method = method;
        }

        var reconnectEnabled = !config.TryGetValue(SignalRConfig.ReconnectEnabled, out var reconnectStr) ||
                               !bool.TryParse(reconnectStr, out var r) || r;

        var reconnectDelayMs = config.TryGetValue(SignalRConfig.ReconnectDelayMs, out var delayStr) &&
                               long.TryParse(delayStr, out var delay)
            ? delay
            : SignalRConfig.DefaultReconnectDelayMs;

        var reconnectMaxDelayMs = config.TryGetValue(SignalRConfig.ReconnectMaxDelayMs, out var maxDelayStr) &&
                                  long.TryParse(maxDelayStr, out var maxDelay)
            ? maxDelay
            : SignalRConfig.DefaultReconnectMaxDelayMs;

        var transport = ParseTransport(config.GetValueOrDefault(SignalRConfig.Transport) ?? SignalRConfig.DefaultTransport);
        var accessToken = config.GetValueOrDefault(SignalRConfig.AccessToken) ?? "";

        var builder = new HubConnectionBuilder()
            .WithUrl(hubUrl, options =>
            {
                options.Transports = transport;

                if (!string.IsNullOrEmpty(accessToken))
                {
                    options.AccessTokenProvider = () => Task.FromResult<string?>(accessToken);
                }

                // Parse custom headers
                var headersJson = config.GetValueOrDefault(SignalRConfig.Headers) ?? "";
                if (!string.IsNullOrEmpty(headersJson))
                {
                    try
                    {
                        var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson);
                        if (headers != null)
                        {
                            foreach (var (key, value) in headers)
                            {
                                options.Headers[key] = value;
                            }
                        }
                    }
                    catch
                    {
                        // Ignore invalid JSON
                    }
                }
            });

        if (reconnectEnabled)
        {
            builder.WithAutomaticReconnect(new ExponentialBackoffRetryPolicy(
                TimeSpan.FromMilliseconds(reconnectDelayMs),
                TimeSpan.FromMilliseconds(reconnectMaxDelayMs)));
        }

        _connection = builder.Build();

        // Subscribe to the hub method - handle different argument counts
        _subscription = _connection.On<string, string>(_method, (key, value) =>
        {
            var msgId = Interlocked.Increment(ref _messageCount);
            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["hub"] = hubUrl, ["method"] = _method },
                SourceOffset = new Dictionary<string, object> { ["messageId"] = msgId },
                Topic = _topic,
                Key = string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key),
                Value = Encoding.UTF8.GetBytes(value),
                Timestamp = DateTimeOffset.UtcNow
            };

            _recordChannel.Writer.TryWrite(record);
        });

        // Also handle single-argument messages
        _connection.On<string>(_method, value =>
        {
            var msgId = Interlocked.Increment(ref _messageCount);
            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["hub"] = hubUrl, ["method"] = _method },
                SourceOffset = new Dictionary<string, object> { ["messageId"] = msgId },
                Topic = _topic,
                Key = null,
                Value = Encoding.UTF8.GetBytes(value),
                Timestamp = DateTimeOffset.UtcNow
            };

            _recordChannel.Writer.TryWrite(record);
        });

        // Handle JSON messages
        _connection.On<object>(_method, obj =>
        {
            var msgId = Interlocked.Increment(ref _messageCount);
            var value = obj is string s ? s : JsonSerializer.Serialize(obj);
            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["hub"] = hubUrl, ["method"] = _method },
                SourceOffset = new Dictionary<string, object> { ["messageId"] = msgId },
                Topic = _topic,
                Key = null,
                Value = Encoding.UTF8.GetBytes(value),
                Timestamp = DateTimeOffset.UtcNow
            };

            _recordChannel.Writer.TryWrite(record);
        });

        // Start connection synchronously
        _connection.StartAsync().GetAwaiter().GetResult();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();
        var reader = _recordChannel.Reader;

        // Try to read all available records without waiting
        while (reader.TryRead(out var record))
        {
            records.Add(record);

            // Limit batch size
            if (records.Count >= 1000)
                break;
        }

        // If no records available, wait briefly for new ones
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromMilliseconds(100));

                if (await reader.WaitToReadAsync(cts.Token))
                {
                    while (reader.TryRead(out var record) && records.Count < 1000)
                    {
                        records.Add(record);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Timeout or cancellation - return what we have
            }
        }

        return records;
    }

    public override void Stop()
    {
        _subscription?.Dispose();
        _subscription = null;
        _recordChannel.Writer.Complete();

        if (_connection != null)
        {
            _connection.StopAsync().GetAwaiter().GetResult();
            _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _connection = null;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _subscription?.Dispose();
            _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose(disposing);
    }

    private static Microsoft.AspNetCore.Http.Connections.HttpTransportType ParseTransport(string transport)
    {
        return transport.ToLowerInvariant() switch
        {
            "websockets" => Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets,
            "serversentevents" => Microsoft.AspNetCore.Http.Connections.HttpTransportType.ServerSentEvents,
            "longpolling" => Microsoft.AspNetCore.Http.Connections.HttpTransportType.LongPolling,
            _ => Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets |
                 Microsoft.AspNetCore.Http.Connections.HttpTransportType.ServerSentEvents |
                 Microsoft.AspNetCore.Http.Connections.HttpTransportType.LongPolling
        };
    }
}
