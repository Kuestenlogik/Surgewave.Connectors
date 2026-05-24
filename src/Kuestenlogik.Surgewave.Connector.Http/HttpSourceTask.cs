namespace Kuestenlogik.Surgewave.Connector.Http;

using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that produces records from HTTP sources.
/// Supports multiple modes:
/// - Poll: Periodically fetches data from an HTTP endpoint
/// - Webhook: Receives push events via registered webhook endpoints
/// - SSE: Auto-detected when server responds with Content-Type: text/event-stream
/// </summary>
public sealed class HttpSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _sourceMode = HttpConnectorConfig.SourceModePoll;
    private string _topic = "";
    private string _connectorName = "";

    // Poll mode fields
    private Uri? _url;
    private long _pollIntervalMs = HttpConnectorConfig.DefaultPollIntervalMs;
    private string _httpMethod = "GET";
    private string _responseMode = HttpConnectorConfig.ResponseModeRaw;
    private HttpClient? _httpClient;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private readonly Dictionary<string, object> _sourcePartition = [];

    // Webhook mode fields
    private ChannelReader<WebhookEvent>? _webhookReader;

    // SSE mode fields (auto-detected)
    private bool _sseMode;
    private string? _lastEventId;
    private long _sseReconnectDelayMs = HttpConnectorConfig.DefaultSseReconnectDelayMs;
    private Channel<SseEvent>? _sseChannel;
    private CancellationTokenSource? _sseCts;
    private Task? _sseReaderTask;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[HttpConnectorConfig.Topic];
        _connectorName = config.TryGetValue("name", out var name) ? name : "http-source";
        _sourceMode = config.TryGetValue(HttpConnectorConfig.SourceMode, out var mode)
            ? mode : HttpConnectorConfig.SourceModePoll;

        if (_sourceMode == HttpConnectorConfig.SourceModeWebhook)
        {
            StartWebhookMode(config);
        }
        else
        {
            StartPollMode(config);
        }
    }

    private void StartPollMode(IDictionary<string, string> config)
    {
        _url = new Uri(config[HttpConnectorConfig.Url]);
        _pollIntervalMs = config.TryGetValue(HttpConnectorConfig.PollIntervalMs, out var interval)
            ? long.Parse(interval) : HttpConnectorConfig.DefaultPollIntervalMs;
        _httpMethod = config.TryGetValue(HttpConnectorConfig.Method, out var method)
            ? method : "GET";
        _responseMode = config.TryGetValue(HttpConnectorConfig.ResponseMode, out var respMode)
            ? respMode : HttpConnectorConfig.ResponseModeRaw;
        _sseReconnectDelayMs = config.TryGetValue(HttpConnectorConfig.SseReconnectDelayMs, out var reconnect)
            ? long.Parse(reconnect) : HttpConnectorConfig.DefaultSseReconnectDelayMs;

        _sourcePartition["url"] = _url.ToString();

        // Try to get stored offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(HttpConnectorConfig.OffsetLastPoll, out var lastPoll))
            {
                _lastPollTime = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(lastPoll));
            }
            if (storedOffset.TryGetValue(HttpConnectorConfig.OffsetLastEventId, out var lastEventId))
            {
                _lastEventId = lastEventId?.ToString();
            }
        }

        _httpClient = new HttpClient();
        _httpClient.Timeout = Timeout.InfiniteTimeSpan; // Allow long-lived SSE connections

        // Parse and apply headers
        if (config.TryGetValue(HttpConnectorConfig.Headers, out var headers) && !string.IsNullOrEmpty(headers))
        {
            foreach (var header in headers.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = header.Split('=', 2);
                if (parts.Length == 2)
                {
                    _httpClient.DefaultRequestHeaders.TryAddWithoutValidation(parts[0].Trim(), parts[1].Trim());
                }
            }
        }
    }

    private void StartWebhookMode(IDictionary<string, string> config)
    {
        var path = config.TryGetValue(HttpConnectorConfig.WebhookPath, out var p)
            ? p.Replace("{name}", _connectorName)
            : HttpConnectorConfig.DefaultWebhookPath.Replace("{name}", _connectorName);

        _sourcePartition["webhook"] = _connectorName;
        _sourcePartition["path"] = path;

        // Register with the webhook registry
        _webhookReader = WebhookRegistry.Instance.Register(_connectorName, path, config);
    }

    public override void Stop()
    {
        if (_sourceMode == HttpConnectorConfig.SourceModeWebhook)
        {
            WebhookRegistry.Instance.Unregister(_connectorName);
        }
        else
        {
            StopSseReader();
            _httpClient?.Dispose();
        }
    }

    private void StopSseReader()
    {
        _sseCts?.Cancel();
        try
        {
            _sseReaderTask?.Wait(TimeSpan.FromSeconds(2));
        }
        catch
        {
            // Ignore cancellation exceptions
        }
        _sseCts?.Dispose();
        _sseCts = null;
        _sseReaderTask = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            StopSseReader();
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sourceMode == HttpConnectorConfig.SourceModeWebhook)
            return await PollWebhookAsync(cancellationToken);

        if (_sseMode)
            return await PollSseAsync(cancellationToken);

        return await PollHttpAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<SourceRecord>> PollWebhookAsync(CancellationToken cancellationToken)
    {
        if (_webhookReader == null)
            return [];

        var records = new List<SourceRecord>();

        // Try to read available events (non-blocking batch)
        while (_webhookReader.TryRead(out var webhookEvent))
        {
            records.Add(CreateWebhookRecord(webhookEvent));

            // Limit batch size
            if (records.Count >= 100)
                break;
        }

        // If no events, wait briefly for one
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100); // Wait max 100ms

                if (await _webhookReader.WaitToReadAsync(cts.Token))
                {
                    if (_webhookReader.TryRead(out var webhookEvent))
                    {
                        records.Add(CreateWebhookRecord(webhookEvent));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout, return empty
            }
        }

        return records;
    }

    private SourceRecord CreateWebhookRecord(WebhookEvent webhookEvent)
    {
        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                [HttpConnectorConfig.OffsetLastPoll] = webhookEvent.ReceivedAt.ToUnixTimeMilliseconds()
            },
            Topic = _topic,
            Value = webhookEvent.Body,
            Timestamp = webhookEvent.ReceivedAt
        };
    }

    private async Task<IReadOnlyList<SourceRecord>> PollHttpAsync(CancellationToken cancellationToken)
    {
        if (_httpClient == null || _url == null)
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
            using var request = new HttpRequestMessage(
                _httpMethod.Equals("POST", StringComparison.OrdinalIgnoreCase) ? HttpMethod.Post : HttpMethod.Get,
                _url);

            // Add Accept header for SSE to allow server to detect client supports it
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(HttpConnectorConfig.SseContentType));
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json", 0.9));
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*", 0.8));

            // Add Last-Event-ID for SSE reconnection
            if (!string.IsNullOrEmpty(_lastEventId))
            {
                request.Headers.TryAddWithoutValidation("Last-Event-ID", _lastEventId);
            }

            var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            // Check if server responded with SSE content type
            var contentType = response.Content.Headers.ContentType?.MediaType;
            if (contentType == HttpConnectorConfig.SseContentType)
            {
                // Switch to SSE mode
                _sseMode = true;
                _sseChannel = Channel.CreateBounded<SseEvent>(new BoundedChannelOptions(1000)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });
                _sseCts = new CancellationTokenSource();

                // Start background SSE reader
                // CA2025: response disposal is handled in ReadSseStreamAsync's finally block
#pragma warning disable CA2025
                _sseReaderTask = Task.Run(() => ReadSseStreamAsync(response, _sseCts.Token), _sseCts.Token);
#pragma warning restore CA2025

                // Return empty for this poll, next poll will read from SSE channel
                return [];
            }

            // Normal HTTP response
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            _lastPollTime = DateTimeOffset.UtcNow;

            var sourceOffset = new Dictionary<string, object>
            {
                [HttpConnectorConfig.OffsetLastPoll] = _lastPollTime.ToUnixTimeMilliseconds()
            };

            var records = new List<SourceRecord>();

            if (_responseMode == HttpConnectorConfig.ResponseModeJsonArray)
            {
                // Parse as JSON array and emit each element as a record
                try
                {
                    using var doc = JsonDocument.Parse(responseBody);
                    if (doc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        var index = 0;
                        foreach (var element in doc.RootElement.EnumerateArray())
                        {
                            var elementJson = element.GetRawText();
                            records.Add(new SourceRecord
                            {
                                SourcePartition = _sourcePartition,
                                SourceOffset = new Dictionary<string, object>
                                {
                                    [HttpConnectorConfig.OffsetLastPoll] = _lastPollTime.ToUnixTimeMilliseconds(),
                                    [HttpConnectorConfig.OffsetIndex] = index++
                                },
                                Topic = _topic,
                                Value = Encoding.UTF8.GetBytes(elementJson),
                                Timestamp = _lastPollTime
                            });
                        }
                    }
                    else
                    {
                        // Not an array, treat as single record
                        records.Add(CreatePollRecord(responseBody, sourceOffset));
                    }
                }
                catch (JsonException)
                {
                    // Invalid JSON, treat as single record
                    records.Add(CreatePollRecord(responseBody, sourceOffset));
                }
            }
            else
            {
                // Raw mode: entire response as one record
                records.Add(CreatePollRecord(responseBody, sourceOffset));
            }

            return records;
        }
        catch (HttpRequestException)
        {
            // HTTP error, wait and try again
            await Task.Delay(1000, cancellationToken);
            return [];
        }
    }

    private async Task<IReadOnlyList<SourceRecord>> PollSseAsync(CancellationToken cancellationToken)
    {
        if (_sseChannel == null)
            return [];

        var records = new List<SourceRecord>();

        // Check if SSE reader task has faulted - need to reconnect
        if (_sseReaderTask != null && _sseReaderTask.IsCompleted)
        {
            if (_sseReaderTask.IsFaulted || _sseReaderTask.IsCanceled)
            {
                // SSE connection lost, switch back to poll mode for reconnection
                StopSseReader();
                _sseMode = false;
                _sseChannel = null;

                // Wait before reconnecting
                await Task.Delay((int)_sseReconnectDelayMs, cancellationToken);
                return [];
            }
        }

        // Try to read available events (non-blocking batch)
        while (_sseChannel.Reader.TryRead(out var sseEvent))
        {
            records.Add(CreateSseRecord(sseEvent));

            // Limit batch size
            if (records.Count >= 100)
                break;
        }

        // If no events, wait briefly for one
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(100); // Wait max 100ms

                if (await _sseChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    if (_sseChannel.Reader.TryRead(out var sseEvent))
                    {
                        records.Add(CreateSseRecord(sseEvent));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal timeout, return empty
            }
        }

        return records;
    }

    private async Task ReadSseStreamAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var reader = new StreamReader(stream, Encoding.UTF8);

            var currentEvent = new SseEventBuilder();

            while (!cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync(cancellationToken);

                if (line == null)
                {
                    // Stream ended
                    break;
                }

                if (string.IsNullOrEmpty(line))
                {
                    // Empty line = dispatch event
                    if (currentEvent.HasData)
                    {
                        var sseEvent = currentEvent.Build();

                        // Update last event ID for reconnection
                        if (!string.IsNullOrEmpty(sseEvent.Id))
                        {
                            _lastEventId = sseEvent.Id;
                        }

                        await _sseChannel!.Writer.WriteAsync(sseEvent, cancellationToken);
                    }
                    currentEvent = new SseEventBuilder();
                    continue;
                }

                // Parse SSE field
                if (line.StartsWith(':'))
                {
                    // Comment, ignore
                    continue;
                }

                var colonIndex = line.IndexOf(':');
                string field, value;

                if (colonIndex == -1)
                {
                    field = line;
                    value = "";
                }
                else
                {
                    field = line[..colonIndex];
                    value = colonIndex + 1 < line.Length ? line[(colonIndex + 1)..] : "";

                    // Remove leading space from value if present
                    if (value.StartsWith(' '))
                    {
                        value = value[1..];
                    }
                }

                switch (field)
                {
                    case "data":
                        currentEvent.AppendData(value);
                        break;
                    case "event":
                        currentEvent.EventType = value;
                        break;
                    case "id":
                        currentEvent.Id = value;
                        break;
                    case "retry":
                        if (long.TryParse(value, out var retryMs))
                        {
                            _sseReconnectDelayMs = retryMs;
                        }
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        finally
        {
            response.Dispose();
            _sseChannel?.Writer.TryComplete();
        }
    }

    private SourceRecord CreateSseRecord(SseEvent sseEvent)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            [HttpConnectorConfig.OffsetLastPoll] = sseEvent.ReceivedAt.ToUnixTimeMilliseconds()
        };

        if (!string.IsNullOrEmpty(sseEvent.Id))
        {
            sourceOffset[HttpConnectorConfig.OffsetLastEventId] = sseEvent.Id;
        }

        // Include event type in headers if present
        var headers = new Dictionary<string, byte[]>();
        if (!string.IsNullOrEmpty(sseEvent.EventType))
        {
            headers["sse-event-type"] = Encoding.UTF8.GetBytes(sseEvent.EventType);
        }
        if (!string.IsNullOrEmpty(sseEvent.Id))
        {
            headers["sse-event-id"] = Encoding.UTF8.GetBytes(sseEvent.Id);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = _topic,
            Value = Encoding.UTF8.GetBytes(sseEvent.Data),
            Timestamp = sseEvent.ReceivedAt,
            Headers = headers.Count > 0 ? headers : null
        };
    }

    private SourceRecord CreatePollRecord(string body, Dictionary<string, object> sourceOffset)
    {
        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Topic = _topic,
            Value = Encoding.UTF8.GetBytes(body),
            Timestamp = _lastPollTime
        };
    }
}
