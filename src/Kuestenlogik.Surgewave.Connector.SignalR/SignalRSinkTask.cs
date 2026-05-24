using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Microsoft.AspNetCore.SignalR.Client;

namespace Kuestenlogik.Surgewave.Connector.SignalR;

/// <summary>
/// SignalR Sink Task - Sends messages to a SignalR hub.
/// </summary>
public sealed class SignalRSinkTask : SinkTask
{
    private HubConnection? _connection;
    private string _method = SignalRConfig.DefaultSendMethod;
    private string _messageFormat = SignalRConfig.DefaultMessageFormat;
    private string _group = "";
    private string _user = "";
    private bool _batchEnabled;
    private int _batchSize = SignalRConfig.DefaultBatchSize;
    private readonly List<SinkRecord> _buffer = [];

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var hubUrl = config[SignalRConfig.HubUrl];
        _method = GetConfigValue(config, SignalRConfig.Method, SignalRConfig.DefaultSendMethod);
        _messageFormat = GetConfigValue(config, SignalRConfig.MessageFormat, SignalRConfig.DefaultMessageFormat);
        _group = GetConfigValue(config, SignalRConfig.TargetGroup, "");
        _user = GetConfigValue(config, SignalRConfig.TargetUser, "");
        _batchEnabled = bool.TryParse(GetConfigValue(config, SignalRConfig.BatchEnabled, "false"), out var b) && b;
        _batchSize = int.Parse(GetConfigValue(config, SignalRConfig.BatchSize, SignalRConfig.DefaultBatchSize.ToString()));

        var reconnectEnabled = bool.TryParse(GetConfigValue(config, SignalRConfig.ReconnectEnabled, "true"), out var r) && r;
        var reconnectDelayMs = long.Parse(GetConfigValue(config, SignalRConfig.ReconnectDelayMs, SignalRConfig.DefaultReconnectDelayMs.ToString()));
        var reconnectMaxDelayMs = long.Parse(GetConfigValue(config, SignalRConfig.ReconnectMaxDelayMs, SignalRConfig.DefaultReconnectMaxDelayMs.ToString()));
        var accessToken = GetConfigValue(config, SignalRConfig.AccessToken, "");
        var transport = ParseTransport(GetConfigValue(config, SignalRConfig.Transport, SignalRConfig.DefaultTransport));

        var builder = new HubConnectionBuilder()
            .WithUrl(hubUrl, options =>
            {
                options.Transports = transport;

                if (!string.IsNullOrEmpty(accessToken))
                {
                    options.AccessTokenProvider = () => Task.FromResult<string?>(accessToken);
                }

                // Parse custom headers
                var headersJson = GetConfigValue(config, SignalRConfig.Headers, "");
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

        // Start connection synchronously
        _connection.StartAsync().GetAwaiter().GetResult();
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        FlushBuffer();
        if (_connection != null)
        {
            _connection.StopAsync().GetAwaiter().GetResult();
            _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _connection = null;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && _connection != null)
        {
            _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_batchEnabled)
        {
            _buffer.AddRange(records);
            if (_buffer.Count >= _batchSize)
            {
                await FlushBufferAsync(cancellationToken);
            }
        }
        else
        {
            foreach (var record in records)
            {
                await SendRecordAsync(record, cancellationToken);
            }
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private void FlushBuffer() => FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0 || _connection == null)
            return;

        // Try to send as batch first
        var batchMethod = _method + "Batch";
        var batchArgs = _buffer.Select(BuildMessageArgs).ToArray();

        try
        {
            await _connection.InvokeAsync(batchMethod, batchArgs, cancellationToken);
        }
        catch
        {
            // If batch method doesn't exist, send individually
            foreach (var record in _buffer)
            {
                await SendRecordAsync(record, cancellationToken);
            }
        }

        _buffer.Clear();
    }

    private async Task SendRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_connection == null || _connection.State != HubConnectionState.Connected)
        {
            throw new InvalidOperationException("Not connected to SignalR hub");
        }

        var args = BuildMessageArgs(record);
        var methodToUse = _method;

        // Handle group/user targeting
        if (!string.IsNullOrEmpty(_group))
        {
            methodToUse = "SendToGroup";
            args = [_group, .. args];
        }
        else if (!string.IsNullOrEmpty(_user))
        {
            methodToUse = "SendToUser";
            args = [_user, .. args];
        }

        await _connection.InvokeAsync(methodToUse, args, cancellationToken);
    }

    private object[] BuildMessageArgs(SinkRecord record)
    {
        var key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : "";
        var value = Encoding.UTF8.GetString(record.Value);

        return _messageFormat.ToLowerInvariant() switch
        {
            "value-only" => [value],
            "json" => [JsonSerializer.Serialize(new
            {
                key,
                value,
                topic = record.Topic,
                partition = record.Partition,
                offset = record.Offset,
                timestamp = record.Timestamp
            })],
            _ => [key, value] // key-value
        };
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
