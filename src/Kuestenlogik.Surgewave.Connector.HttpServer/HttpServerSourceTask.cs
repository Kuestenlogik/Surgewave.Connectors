using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kuestenlogik.Surgewave.Connector.HttpServer;

/// <summary>
/// Source task that runs an embedded HTTP server to receive requests.
/// Accepted requests are parked in an in-memory queue until the next poll, so the
/// 202 response means "queued for production", not "durably stored". The queue is
/// bounded (<see cref="HttpServerConnectorConfig.SourceMaxQueueSize"/>): once it is
/// full, further requests are rejected with 503 instead of piling up in memory.
/// </summary>
public sealed class HttpServerSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _sourcePath = "";
    private HashSet<string> _allowedMethods = new(StringComparer.OrdinalIgnoreCase);
    private bool _includeHeaders;
    private bool _includeQueryParams;
    private bool _authEnabled;
    private string _authType = HttpServerConnectorConfig.AuthTypeNone;
    private HashSet<string> _apiKeys = [];
    private string _apiKeyHeader = HttpServerConnectorConfig.DefaultApiKeyHeader;
    private Dictionary<string, string> _basicUsers = [];

    private WebApplication? _app;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private int _pendingCount;
    private int _maxQueueSize = HttpServerConnectorConfig.DefaultSourceMaxQueueSize;
    private string _serverId = "";
    private long _messageCounter;

    public override void Start(IDictionary<string, string> config)
    {
        // Parse configuration
        _topic = config[HttpServerConnectorConfig.SourceTopic];

        var host = config.TryGetValue(HttpServerConnectorConfig.Host, out var h) ? h : HttpServerConnectorConfig.DefaultHost;
        var port = config.TryGetValue(HttpServerConnectorConfig.Port, out var p) && int.TryParse(p, out var portNum)
            ? portNum : HttpServerConnectorConfig.DefaultPort;
        var basePath = config.TryGetValue(HttpServerConnectorConfig.BasePath, out var bp) ? bp.TrimEnd('/') : HttpServerConnectorConfig.DefaultBasePath;

        // Task configs are not merged with the ConfigDef defaults, so the source partition
        // is derived from the effective host/port resolved here - never from raw lookups.
        _serverId = $"{host}:{port.ToString(CultureInfo.InvariantCulture)}";

        _maxQueueSize = config.TryGetValue(HttpServerConnectorConfig.SourceMaxQueueSize, out var mq)
            && int.TryParse(mq, NumberStyles.Integer, CultureInfo.InvariantCulture, out var maxQueue) && maxQueue > 0
            ? maxQueue : HttpServerConnectorConfig.DefaultSourceMaxQueueSize;

        _sourcePath = config.TryGetValue(HttpServerConnectorConfig.SourcePath, out var sp) ? sp : HttpServerConnectorConfig.DefaultSourcePath;
        if (!_sourcePath.StartsWith('/')) _sourcePath = "/" + _sourcePath;

        var methods = config.TryGetValue(HttpServerConnectorConfig.SourceMethods, out var m) ? m : HttpServerConnectorConfig.DefaultSourceMethods;
        _allowedMethods = methods.Split(',').Select(x => x.Trim().ToUpperInvariant()).ToHashSet();

        _includeHeaders = !config.TryGetValue(HttpServerConnectorConfig.SourceIncludeHeaders, out var ih) || !bool.TryParse(ih, out var includeH) || includeH;
        _includeQueryParams = !config.TryGetValue(HttpServerConnectorConfig.SourceIncludeQueryParams, out var iq) || !bool.TryParse(iq, out var includeQ) || includeQ;

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

        // Register the ingest endpoint
        var fullPath = basePath + _sourcePath;
        _app.Map(fullPath, (Delegate)HandleRequest);

        // Health check endpoint
        _app.MapGet(basePath + "/health", () => Results.Ok(new { status = "healthy", timestamp = DateTimeOffset.UtcNow }));

        // Start the server
        _app.StartAsync().GetAwaiter().GetResult();
    }

    private async Task<IResult> HandleRequest(HttpContext context)
    {
        // Check method
        if (!_allowedMethods.Contains(context.Request.Method))
        {
            return Results.StatusCode(405);
        }

        // Authentication
        if (_authEnabled && !ValidateAuth(context))
        {
            return Results.Unauthorized();
        }

        // Read body
        using var ms = new MemoryStream();
        await context.Request.Body.CopyToAsync(ms);
        var body = ms.ToArray();

        // Build message
        var message = new Dictionary<string, object>
        {
            ["body"] = body.Length > 0 ? Encoding.UTF8.GetString(body) : "",
            ["method"] = context.Request.Method,
            ["path"] = context.Request.Path.Value ?? "",
            ["content_type"] = context.Request.ContentType ?? "",
            ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        if (_includeHeaders)
        {
            var headers = new Dictionary<string, string>();
            foreach (var header in context.Request.Headers)
            {
                // Skip sensitive headers
                if (!header.Key.Equals("Authorization", StringComparison.OrdinalIgnoreCase) &&
                    !header.Key.Equals(_apiKeyHeader, StringComparison.OrdinalIgnoreCase))
                {
                    headers[header.Key] = header.Value.ToString();
                }
            }
            message["headers"] = headers;
        }

        if (_includeQueryParams && context.Request.QueryString.HasValue)
        {
            var queryParams = new Dictionary<string, string>();
            foreach (var param in context.Request.Query)
            {
                queryParams[param.Key] = param.Value.ToString();
            }
            message["query"] = queryParams;
        }

        // Back-pressure: an accepted record only lives in memory until the next poll,
        // so refuse the request instead of queueing more than we are willing to hold.
        if (Interlocked.Increment(ref _pendingCount) > _maxQueueSize)
        {
            Interlocked.Decrement(ref _pendingCount);
            return Results.StatusCode((int)HttpStatusCode.ServiceUnavailable);
        }

        // Create source record
        var messageId = Interlocked.Increment(ref _messageCounter);
        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["server"] = _serverId },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = messageId },
            Topic = _topic,
            Value = JsonSerializer.SerializeToUtf8Bytes(message),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["http_method"] = Encoding.UTF8.GetBytes(context.Request.Method),
                ["http_path"] = Encoding.UTF8.GetBytes(context.Request.Path.Value ?? ""),
                ["content_type"] = Encoding.UTF8.GetBytes(context.Request.ContentType ?? "application/octet-stream")
            }
        };

        _pendingRecords.Enqueue(record);

        return Results.Accepted(value: new { message_id = messageId, status = "accepted" });
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

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
            Interlocked.Decrement(ref _pendingCount);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override void Stop()
    {
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
}
