using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kuestenlogik.Surgewave.Connector.HttpServer;

/// <summary>
/// Sink task that runs an embedded HTTP server to serve topic data.
/// </summary>
public sealed class HttpServerSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private IDictionary<string, string> _config = new Dictionary<string, string>();
    private HashSet<string> _topics = [];
    private int _maxMessages;
    private int _defaultLimit;
    private bool _enableStreaming;
    private bool _authEnabled;
    private string _authType = HttpServerConnectorConfig.AuthTypeNone;
    private HashSet<string> _apiKeys = [];
    private string _apiKeyHeader = HttpServerConnectorConfig.DefaultApiKeyHeader;
    private Dictionary<string, string> _basicUsers = [];

    private WebApplication? _app;

    // Message buffers per topic (ring buffer behavior)
    private readonly ConcurrentDictionary<string, MessageBuffer> _messageBuffers = new();

    // SSE subscribers
    private readonly ConcurrentDictionary<string, List<Channel<SinkRecord>>> _sseSubscribers = new();
    private readonly object _subscriberLock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Parse configuration
        var topicsStr = config[HttpServerConnectorConfig.SinkTopics];
        _topics = topicsStr.Split(',').Select(t => t.Trim()).Where(t => !string.IsNullOrEmpty(t)).ToHashSet();

        var host = config.TryGetValue(HttpServerConnectorConfig.Host, out var h) ? h : HttpServerConnectorConfig.DefaultHost;
        var port = config.TryGetValue(HttpServerConnectorConfig.Port, out var p) && int.TryParse(p, out var portNum)
            ? portNum : HttpServerConnectorConfig.DefaultPort;
        var basePath = config.TryGetValue(HttpServerConnectorConfig.BasePath, out var bp) ? bp.TrimEnd('/') : HttpServerConnectorConfig.DefaultBasePath;

        _maxMessages = config.TryGetValue(HttpServerConnectorConfig.SinkMaxMessages, out var mm) && int.TryParse(mm, out var max)
            ? max : HttpServerConnectorConfig.DefaultSinkMaxMessages;
        _defaultLimit = config.TryGetValue(HttpServerConnectorConfig.SinkDefaultLimit, out var dl) && int.TryParse(dl, out var limit)
            ? limit : HttpServerConnectorConfig.DefaultSinkDefaultLimit;
        _enableStreaming = config.TryGetValue(HttpServerConnectorConfig.SinkEnableStreaming, out var se) && bool.TryParse(se, out var stream) && stream;

        // Auth config
        _authEnabled = config.TryGetValue(HttpServerConnectorConfig.AuthEnabled, out var ae) && bool.TryParse(ae, out var enabled) && enabled;
        if (_authEnabled)
        {
            _authType = config.TryGetValue(HttpServerConnectorConfig.AuthType, out var at) ? at : HttpServerConnectorConfig.AuthTypeNone;
            _apiKeyHeader = config.TryGetValue(HttpServerConnectorConfig.AuthApiKeyHeader, out var akh) ? akh : HttpServerConnectorConfig.DefaultApiKeyHeader;

            if (_authType == HttpServerConnectorConfig.AuthTypeApiKey && config.TryGetValue(HttpServerConnectorConfig.AuthApiKeys, out var keys))
            {
                _apiKeys = keys.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToHashSet();
            }

            if (_authType == HttpServerConnectorConfig.AuthTypeBasic && config.TryGetValue(HttpServerConnectorConfig.AuthBasicUsers, out var users))
            {
                foreach (var userPass in users.Split(','))
                {
                    var parts = userPass.Split(':', 2);
                    if (parts.Length == 2)
                    {
                        _basicUsers[parts[0].Trim()] = parts[1];
                    }
                }
            }
        }

        // CORS config
        var enableCors = config.TryGetValue(HttpServerConnectorConfig.EnableCors, out var ec) && bool.TryParse(ec, out var cors) && cors;
        var corsOrigins = config.TryGetValue(HttpServerConnectorConfig.CorsOrigins, out var co) ? co : "*";

        // Initialize message buffers for each topic
        foreach (var topic in _topics)
        {
            _messageBuffers[topic] = new MessageBuffer(_maxMessages);
            _sseSubscribers[topic] = [];
        }

        // Build and start the HTTP server
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Parse(host == "localhost" ? "127.0.0.1" : host), port);
        });

        if (enableCors)
        {
            builder.Services.AddCors(options =>
            {
                options.AddDefaultPolicy(policy =>
                {
                    if (corsOrigins == "*")
                        policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
                    else
                        policy.WithOrigins(corsOrigins.Split(',')).AllowAnyMethod().AllowAnyHeader();
                });
            });
        }

        _app = builder.Build();

        if (enableCors)
        {
            _app.UseCors();
        }

        // Register endpoints
        // List available topics
        _app.MapGet(basePath + "/topics", HandleListTopics);

        // Get messages from a topic
        _app.MapGet(basePath + "/topics/{topic}", HandleGetMessages);

        // Get a specific message by offset
        _app.MapGet(basePath + "/topics/{topic}/{offset:long}", HandleGetMessage);

        // SSE streaming endpoint
        if (_enableStreaming)
        {
            _app.MapGet(basePath + "/topics/{topic}/stream", HandleStream);
        }

        // Health check endpoint
        _app.MapGet(basePath + "/health", () => Results.Ok(new { status = "healthy", timestamp = DateTimeOffset.UtcNow }));

        // Start the server
        _app.StartAsync().GetAwaiter().GetResult();
    }

    private IResult HandleListTopics(HttpContext context)
    {
        if (_authEnabled && !ValidateAuth(context))
        {
            return Results.Unauthorized();
        }

        var topicInfos = _topics.Select(topic =>
        {
            var buffer = _messageBuffers.GetValueOrDefault(topic);
            return new
            {
                name = topic,
                message_count = buffer?.Count ?? 0,
                oldest_offset = buffer?.OldestOffset ?? -1,
                newest_offset = buffer?.NewestOffset ?? -1
            };
        });

        return Results.Ok(new { topics = topicInfos });
    }

    private IResult HandleGetMessages(HttpContext context, string topic)
    {
        if (_authEnabled && !ValidateAuth(context))
        {
            return Results.Unauthorized();
        }

        if (!_topics.Contains(topic))
        {
            return Results.NotFound(new { error = "Topic not found", topic });
        }

        // Parse query parameters
        var fromOffset = context.Request.Query.TryGetValue("from", out var fromStr) && long.TryParse(fromStr, out var from)
            ? from : (long?)null;
        var limitParam = context.Request.Query.TryGetValue("limit", out var limitStr) && int.TryParse(limitStr, out var lim)
            ? lim : _defaultLimit;

        var buffer = _messageBuffers.GetValueOrDefault(topic);
        if (buffer == null)
        {
            return Results.Ok(new { topic, messages = Array.Empty<object>(), count = 0 });
        }

        var messages = buffer.GetMessages(fromOffset, limitParam);
        var result = messages.Select(FormatMessage);

        return Results.Ok(new
        {
            topic,
            messages = result,
            count = messages.Count,
            oldest_offset = buffer.OldestOffset,
            newest_offset = buffer.NewestOffset
        });
    }

    private IResult HandleGetMessage(HttpContext context, string topic, long offset)
    {
        if (_authEnabled && !ValidateAuth(context))
        {
            return Results.Unauthorized();
        }

        if (!_topics.Contains(topic))
        {
            return Results.NotFound(new { error = "Topic not found", topic });
        }

        var buffer = _messageBuffers.GetValueOrDefault(topic);
        var message = buffer?.GetMessage(offset);

        if (message == null)
        {
            return Results.NotFound(new { error = "Message not found", topic, offset });
        }

        return Results.Ok(FormatMessage(message));
    }

    private async Task HandleStream(HttpContext context, string topic)
    {
        if (_authEnabled && !ValidateAuth(context))
        {
            context.Response.StatusCode = 401;
            return;
        }

        if (!_topics.Contains(topic))
        {
            context.Response.StatusCode = 404;
            await context.Response.WriteAsJsonAsync(new { error = "Topic not found", topic });
            return;
        }

        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";

        var channel = Channel.CreateUnbounded<SinkRecord>();

        lock (_subscriberLock)
        {
            if (_sseSubscribers.TryGetValue(topic, out var subscribers))
            {
                subscribers.Add(channel);
            }
        }

        try
        {
            await foreach (var record in channel.Reader.ReadAllAsync(context.RequestAborted))
            {
                var data = JsonSerializer.Serialize(FormatMessage(record));
                await context.Response.WriteAsync($"data: {data}\n\n", context.RequestAborted);
                await context.Response.Body.FlushAsync(context.RequestAborted);
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }
        finally
        {
            lock (_subscriberLock)
            {
                if (_sseSubscribers.TryGetValue(topic, out var subscribers))
                {
                    subscribers.Remove(channel);
                }
            }
            channel.Writer.Complete();
        }
    }

    private static object FormatMessage(SinkRecord record)
    {
        string? valueStr = null;
        if (record.Value != null)
        {
            try
            {
                valueStr = Encoding.UTF8.GetString(record.Value);
            }
            catch
            {
                valueStr = Convert.ToBase64String(record.Value);
            }
        }

        string? keyStr = null;
        if (record.Key != null)
        {
            try
            {
                keyStr = Encoding.UTF8.GetString(record.Key);
            }
            catch
            {
                keyStr = Convert.ToBase64String(record.Key);
            }
        }

        return new
        {
            offset = record.Offset,
            partition = record.Partition,
            timestamp = record.Timestamp.ToUnixTimeMilliseconds(),
            key = keyStr,
            value = valueStr,
            headers = record.Headers?.ToDictionary(
                h => h.Key,
                h => Encoding.UTF8.GetString(h.Value))
        };
    }

    private bool ValidateAuth(HttpContext context)
    {
        return _authType switch
        {
            HttpServerConnectorConfig.AuthTypeApiKey => ValidateApiKey(context),
            HttpServerConnectorConfig.AuthTypeBasic => ValidateBasicAuth(context),
            _ => true
        };
    }

    private bool ValidateApiKey(HttpContext context)
    {
        if (!context.Request.Headers.TryGetValue(_apiKeyHeader, out var apiKey))
            return false;

        return _apiKeys.Contains(apiKey.ToString());
    }

    private bool ValidateBasicAuth(HttpContext context)
    {
        if (!context.Request.Headers.TryGetValue("Authorization", out var authHeader))
            return false;

        var auth = authHeader.ToString();
        if (!auth.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
            return false;

        try
        {
            var credentials = Encoding.UTF8.GetString(Convert.FromBase64String(auth[6..]));
            var parts = credentials.Split(':', 2);
            if (parts.Length != 2)
                return false;

            return _basicUsers.TryGetValue(parts[0], out var pass) && pass == parts[1];
        }
        catch
        {
            return false;
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (_messageBuffers.TryGetValue(record.Topic, out var buffer))
            {
                buffer.Add(record);

                // Notify SSE subscribers
                if (_enableStreaming && _sseSubscribers.TryGetValue(record.Topic, out var subscribers))
                {
                    lock (_subscriberLock)
                    {
                        foreach (var channel in subscribers.ToList())
                        {
                            if (!channel.Writer.TryWrite(record))
                            {
                                // Subscriber channel is full, remove it
                                subscribers.Remove(channel);
                                channel.Writer.Complete();
                            }
                        }
                    }
                }
            }
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // No external flushing needed - messages are in memory
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        // Complete all subscriber channels
        lock (_subscriberLock)
        {
            foreach (var (_, subscribers) in _sseSubscribers)
            {
                foreach (var channel in subscribers)
                {
                    channel.Writer.Complete();
                }
                subscribers.Clear();
            }
        }

        DisposeApp();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeApp();
        }
        base.Dispose(disposing);
    }

    private void DisposeApp()
    {
        if (_app != null)
        {
            _app.StopAsync().GetAwaiter().GetResult();
            _app.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _app = null;
        }
    }

    /// <summary>
    /// Ring buffer for storing messages with automatic eviction of old messages.
    /// </summary>
    private sealed class MessageBuffer
    {
        private readonly SinkRecord?[] _buffer;
        private readonly int _capacity;
        private readonly object _lock = new();
        private long _head; // Next write position
        private long _tail; // Oldest message position
        private int _count;

        public MessageBuffer(int capacity)
        {
            _capacity = capacity;
            _buffer = new SinkRecord[capacity];
        }

        public int Count
        {
            get
            {
                lock (_lock) return _count;
            }
        }

        public long OldestOffset
        {
            get
            {
                lock (_lock)
                {
                    if (_count == 0) return -1;
                    return _buffer[_tail % _capacity]?.Offset ?? -1;
                }
            }
        }

        public long NewestOffset
        {
            get
            {
                lock (_lock)
                {
                    if (_count == 0) return -1;
                    var pos = (_head - 1 + _capacity) % _capacity;
                    return _buffer[pos]?.Offset ?? -1;
                }
            }
        }

        public void Add(SinkRecord record)
        {
            lock (_lock)
            {
                var pos = (int)(_head % _capacity);
                _buffer[pos] = record;
                _head++;

                if (_count < _capacity)
                {
                    _count++;
                }
                else
                {
                    _tail++; // Evict oldest
                }
            }
        }

        public SinkRecord? GetMessage(long offset)
        {
            lock (_lock)
            {
                for (var i = 0; i < _count; i++)
                {
                    var pos = (int)((_tail + i) % _capacity);
                    var record = _buffer[pos];
                    if (record?.Offset == offset)
                        return record;
                }
                return null;
            }
        }

        public List<SinkRecord> GetMessages(long? fromOffset, int limit)
        {
            lock (_lock)
            {
                var result = new List<SinkRecord>();
                var startIndex = 0;

                if (fromOffset.HasValue)
                {
                    for (var i = 0; i < _count; i++)
                    {
                        var pos = (int)((_tail + i) % _capacity);
                        if (_buffer[pos]?.Offset >= fromOffset.Value)
                        {
                            startIndex = i;
                            break;
                        }
                    }
                }

                for (var i = startIndex; i < _count && result.Count < limit; i++)
                {
                    var pos = (int)((_tail + i) % _capacity);
                    var record = _buffer[pos];
                    if (record != null)
                        result.Add(record);
                }

                return result;
            }
        }
    }
}
