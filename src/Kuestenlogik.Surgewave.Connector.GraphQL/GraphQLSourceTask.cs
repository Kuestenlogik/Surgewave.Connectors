using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using GraphQL;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.SystemTextJson;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL;

/// <summary>
/// Task that produces records from GraphQL queries or subscriptions.
/// </summary>
public sealed class GraphQLSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private GraphQLHttpClient? _client;
    private string _sourceMode = GraphQLConnectorConfig.DefaultSourceMode;
    private string _query = "";
    private string _operationName = "";
    private Dictionary<string, object?>? _variables;
    private string _topic = "";
    private string _dataPath = "";
    private string _idField = "";
    private string _timestampField = "";
    private int _pollIntervalMs = GraphQLConnectorConfig.DefaultPollIntervalMs;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private string? _lastId;
    private string? _lastTimestamp;
    private readonly Dictionary<string, object> _sourcePartition = [];

    // Subscription fields
    private ClientWebSocket? _webSocket;
    private string _wsEndpoint = "";
    private string _authHeader = GraphQLConnectorConfig.DefaultAuthHeader;
    private string _authToken = "";
    private Channel<JsonElement>? _subscriptionChannel;
    private CancellationTokenSource? _subscriptionCts;
    private Task? _subscriptionTask;

    public override void Start(IDictionary<string, string> config)
    {
        var endpoint = config[GraphQLConnectorConfig.EndpointConfig];
        _topic = config[GraphQLConnectorConfig.TopicConfig];
        _query = config[GraphQLConnectorConfig.QueryConfig];
        _sourceMode = config.TryGetValue(GraphQLConnectorConfig.SourceModeConfig, out var mode)
            ? mode : GraphQLConnectorConfig.DefaultSourceMode;

        _operationName = config.TryGetValue(GraphQLConnectorConfig.OperationNameConfig, out var opName)
            ? opName : "";
        _dataPath = config.TryGetValue(GraphQLConnectorConfig.DataPathConfig, out var dataPath)
            ? dataPath : "";
        _idField = config.TryGetValue(GraphQLConnectorConfig.IdFieldConfig, out var idField)
            ? idField : "";
        _timestampField = config.TryGetValue(GraphQLConnectorConfig.TimestampFieldConfig, out var tsField)
            ? tsField : "";
        _pollIntervalMs = config.TryGetValue(GraphQLConnectorConfig.PollIntervalMsConfig, out var pollInterval)
            ? int.Parse(pollInterval) : GraphQLConnectorConfig.DefaultPollIntervalMs;

        _authHeader = config.TryGetValue(GraphQLConnectorConfig.AuthHeaderConfig, out var authHeader)
            ? authHeader : GraphQLConnectorConfig.DefaultAuthHeader;
        _authToken = config.TryGetValue(GraphQLConnectorConfig.AuthTokenConfig, out var authToken)
            ? authToken : "";

        if (config.TryGetValue(GraphQLConnectorConfig.VariablesConfig, out var variables) &&
            !string.IsNullOrWhiteSpace(variables))
        {
            _variables = JsonSerializer.Deserialize<Dictionary<string, object?>>(variables);
        }

        var timeoutMs = config.TryGetValue(GraphQLConnectorConfig.TimeoutMsConfig, out var timeout)
            ? int.Parse(timeout) : GraphQLConnectorConfig.DefaultTimeoutMs;

        _sourcePartition["endpoint"] = endpoint;
        _sourcePartition["query"] = _query;

        // Restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(GraphQLConnectorConfig.OffsetLastId, out var lastId))
                _lastId = lastId?.ToString();
            if (storedOffset.TryGetValue(GraphQLConnectorConfig.OffsetLastTimestamp, out var lastTs))
                _lastTimestamp = lastTs?.ToString();
            if (storedOffset.TryGetValue(GraphQLConnectorConfig.OffsetLastPoll, out var lastPoll))
                _lastPollTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastPoll));
        }

        // Create HTTP client
        _client = new GraphQLHttpClient(endpoint, new SystemTextJsonSerializer());
        _client.HttpClient.Timeout = TimeSpan.FromMilliseconds(timeoutMs);

        // Add auth header
        if (!string.IsNullOrWhiteSpace(_authToken))
        {
            _client.HttpClient.DefaultRequestHeaders.Add(_authHeader, _authToken);
        }

        // Add custom headers
        if (config.TryGetValue(GraphQLConnectorConfig.HeadersConfig, out var headers) &&
            !string.IsNullOrWhiteSpace(headers))
        {
            foreach (var header in headers.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = header.Split('=', 2);
                if (parts.Length == 2)
                {
                    _client.HttpClient.DefaultRequestHeaders.TryAddWithoutValidation(
                        parts[0].Trim(), parts[1].Trim());
                }
            }
        }

        // Start subscription mode if configured
        if (_sourceMode == GraphQLConnectorConfig.SourceModeSubscription)
        {
            _wsEndpoint = config[GraphQLConnectorConfig.WebSocketEndpointConfig];
            StartSubscription();
        }
    }

    private void StartSubscription()
    {
        _subscriptionChannel = Channel.CreateBounded<JsonElement>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
        _subscriptionCts = new CancellationTokenSource();
        _subscriptionTask = Task.Run(() => RunSubscriptionAsync(_subscriptionCts.Token), _subscriptionCts.Token);
    }

    private async Task RunSubscriptionAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                _webSocket = new ClientWebSocket();

                // Add auth header to WebSocket
                if (!string.IsNullOrWhiteSpace(_authToken))
                {
                    _webSocket.Options.SetRequestHeader(_authHeader, _authToken);
                }

                await _webSocket.ConnectAsync(new Uri(_wsEndpoint), cancellationToken);

                // Send connection_init (graphql-ws protocol)
                var initMessage = JsonSerializer.Serialize(new { type = "connection_init" });
                await _webSocket.SendAsync(
                    Encoding.UTF8.GetBytes(initMessage),
                    WebSocketMessageType.Text,
                    true,
                    cancellationToken);

                // Wait for connection_ack
                var buffer = new byte[4096];
                var result = await _webSocket.ReceiveAsync(buffer, cancellationToken);
                var ackMessage = Encoding.UTF8.GetString(buffer, 0, result.Count);
                var ack = JsonSerializer.Deserialize<JsonElement>(ackMessage);
                if (ack.GetProperty("type").GetString() != "connection_ack")
                {
                    throw new InvalidOperationException("Connection not acknowledged");
                }

                // Send subscribe message
                var subscribePayload = new Dictionary<string, object?>
                {
                    ["query"] = _query
                };
                if (!string.IsNullOrWhiteSpace(_operationName))
                    subscribePayload["operationName"] = _operationName;
                if (_variables != null)
                    subscribePayload["variables"] = _variables;

                var subscribeMessage = JsonSerializer.Serialize(new
                {
                    id = "1",
                    type = "subscribe",
                    payload = subscribePayload
                });
                await _webSocket.SendAsync(
                    Encoding.UTF8.GetBytes(subscribeMessage),
                    WebSocketMessageType.Text,
                    true,
                    cancellationToken);

                // Read subscription data
                var dataBuffer = new List<byte>();
                while (!cancellationToken.IsCancellationRequested &&
                       _webSocket.State == WebSocketState.Open)
                {
                    result = await _webSocket.ReceiveAsync(buffer, cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Close)
                        break;

                    dataBuffer.AddRange(buffer.AsSpan(0, result.Count).ToArray());

                    if (result.EndOfMessage)
                    {
                        var messageJson = Encoding.UTF8.GetString(dataBuffer.ToArray());
                        dataBuffer.Clear();

                        var message = JsonSerializer.Deserialize<JsonElement>(messageJson);
                        var type = message.GetProperty("type").GetString();

                        switch (type)
                        {
                            case "next":
                                var payload = message.GetProperty("payload");
                                if (payload.TryGetProperty("data", out var data))
                                {
                                    await _subscriptionChannel!.Writer.WriteAsync(data, cancellationToken);
                                }
                                break;
                            case "error":
                            case "complete":
                                // Connection ended
                                break;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (WebSocketException)
            {
                // Reconnect after delay
                await Task.Delay(5000, cancellationToken);
            }
            finally
            {
                if (_webSocket?.State == WebSocketState.Open)
                {
                    try
                    {
                        await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                    }
                    catch { }
                }
                _webSocket?.Dispose();
                _webSocket = null;
            }
        }
    }

    public override void Stop()
    {
        _subscriptionCts?.Cancel();
        try
        {
            _subscriptionTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch { }
        _subscriptionCts?.Dispose();
        _client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _subscriptionCts?.Cancel();
            _subscriptionCts?.Dispose();
            _webSocket?.Dispose();
            _client?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sourceMode == GraphQLConnectorConfig.SourceModeSubscription)
            return await PollSubscriptionAsync(cancellationToken);

        return await PollQueryAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<SourceRecord>> PollQueryAsync(CancellationToken cancellationToken)
    {
        if (_client == null)
            return [];

        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;

        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        try
        {
            // Build variables with cursor if configured
            var variables = _variables != null
                ? new Dictionary<string, object?>(_variables)
                : new Dictionary<string, object?>();

            if (!string.IsNullOrWhiteSpace(_timestampField) && !string.IsNullOrWhiteSpace(_lastTimestamp))
            {
                variables["after"] = _lastTimestamp;
            }
            else if (!string.IsNullOrWhiteSpace(_idField) && !string.IsNullOrWhiteSpace(_lastId))
            {
                variables["after"] = _lastId;
            }

            var request = new GraphQLRequest
            {
                Query = _query,
                OperationName = string.IsNullOrWhiteSpace(_operationName) ? null : _operationName,
                Variables = variables.Count > 0 ? variables : null
            };

            var response = await _client.SendQueryAsync<JsonElement>(request, cancellationToken);

            if (response.Errors?.Length > 0)
            {
                // Log errors but don't fail
                _lastPollTime = DateTimeOffset.UtcNow;
                return [];
            }

            var records = new List<SourceRecord>();
            var dataElements = ExtractDataElements(response.Data);

            foreach (var element in dataElements)
            {
                var record = CreateRecord(element);
                records.Add(record);

                // Track cursor
                if (!string.IsNullOrWhiteSpace(_timestampField) &&
                    element.TryGetProperty(_timestampField, out var tsValue))
                {
                    _lastTimestamp = tsValue.ToString();
                }
                if (!string.IsNullOrWhiteSpace(_idField) &&
                    element.TryGetProperty(_idField, out var idValue))
                {
                    _lastId = idValue.ToString();
                }
            }

            _lastPollTime = DateTimeOffset.UtcNow;
            return records;
        }
        catch (GraphQLHttpRequestException)
        {
            await Task.Delay(1000, cancellationToken);
            return [];
        }
    }

    private async Task<IReadOnlyList<SourceRecord>> PollSubscriptionAsync(CancellationToken cancellationToken)
    {
        if (_subscriptionChannel == null)
            return [];

        var records = new List<SourceRecord>();

        // Try to read available events
        while (_subscriptionChannel.Reader.TryRead(out var data))
        {
            var elements = ExtractDataElements(data);
            foreach (var element in elements)
            {
                records.Add(CreateRecord(element));
            }

            if (records.Count >= 100)
                break;
        }

        // If no events, wait briefly for one
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100);

                if (await _subscriptionChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    if (_subscriptionChannel.Reader.TryRead(out var data))
                    {
                        var elements = ExtractDataElements(data);
                        foreach (var element in elements)
                        {
                            records.Add(CreateRecord(element));
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout
            }
        }

        return records;
    }

    private List<JsonElement> ExtractDataElements(JsonElement data)
    {
        var current = data;

        // Navigate to data path if specified
        if (!string.IsNullOrWhiteSpace(_dataPath))
        {
            foreach (var part in _dataPath.Split('.'))
            {
                if (current.TryGetProperty(part, out var next))
                    current = next;
                else
                    return [];
            }
        }

        // Return array elements or single element
        if (current.ValueKind == JsonValueKind.Array)
        {
            return current.EnumerateArray().ToList();
        }

        return [current];
    }

    private SourceRecord CreateRecord(JsonElement element)
    {
        var offset = new Dictionary<string, object>
        {
            [GraphQLConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        if (!string.IsNullOrWhiteSpace(_lastId))
            offset[GraphQLConnectorConfig.OffsetLastId] = _lastId;
        if (!string.IsNullOrWhiteSpace(_lastTimestamp))
            offset[GraphQLConnectorConfig.OffsetLastTimestamp] = _lastTimestamp;

        byte[]? key = null;
        if (!string.IsNullOrWhiteSpace(_idField) && element.TryGetProperty(_idField, out var idValue))
        {
            key = Encoding.UTF8.GetBytes(idValue.ToString());
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = offset,
            Topic = _topic,
            Key = key,
            Value = Encoding.UTF8.GetBytes(element.GetRawText()),
            Timestamp = DateTimeOffset.UtcNow
        };
    }
}
