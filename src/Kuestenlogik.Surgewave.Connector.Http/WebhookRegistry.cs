namespace Kuestenlogik.Surgewave.Connector.Http;

using System.Collections.Concurrent;
using System.Threading.Channels;

/// <summary>
/// Represents a webhook event received from an external source.
/// </summary>
public sealed record WebhookEvent(
    string ConnectorName,
    byte[] Body,
    IDictionary<string, string> Headers,
    DateTimeOffset ReceivedAt);

/// <summary>
/// Registration information for a webhook endpoint.
/// </summary>
internal sealed class WebhookRegistration
{
    public required string Name { get; init; }
    public required string Path { get; init; }
    public string? Secret { get; init; }
    public string SignatureHeader { get; init; } = HttpConnectorConfig.DefaultSignatureHeader;
    public string SignatureAlgorithm { get; init; } = HttpConnectorConfig.DefaultSignatureAlgorithm;
    public bool ValidateTimestamp { get; init; }
    public string TimestampHeader { get; init; } = HttpConnectorConfig.DefaultTimestampHeader;
    public long TimestampToleranceMs { get; init; } = HttpConnectorConfig.DefaultTimestampToleranceMs;
    public required Channel<WebhookEvent> EventChannel { get; init; }
}

/// <summary>
/// Manages webhook endpoint registrations and event routing.
/// Thread-safe singleton for managing webhook endpoints across connectors.
/// </summary>
public sealed class WebhookRegistry
{
    private static readonly Lazy<WebhookRegistry> _instance = new(() => new WebhookRegistry());
    public static WebhookRegistry Instance => _instance.Value;

    private readonly ConcurrentDictionary<string, WebhookRegistration> _registrations = new();
    private readonly ConcurrentDictionary<string, string> _pathToName = new();

    private WebhookRegistry() { }

    /// <summary>
    /// Register a webhook endpoint for a connector.
    /// </summary>
    /// <param name="name">Unique connector name.</param>
    /// <param name="path">HTTP path for the webhook endpoint.</param>
    /// <param name="config">Configuration dictionary with webhook settings.</param>
    /// <returns>ChannelReader for consuming webhook events.</returns>
    public ChannelReader<WebhookEvent> Register(string name, string path, IDictionary<string, string> config)
    {
        var channel = Channel.CreateBounded<WebhookEvent>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        var registration = new WebhookRegistration
        {
            Name = name,
            Path = NormalizePath(path),
            Secret = config.TryGetValue(HttpConnectorConfig.WebhookSecret, out var s) ? s : null,
            SignatureHeader = config.TryGetValue(HttpConnectorConfig.WebhookSignatureHeader, out var sh)
                ? sh : HttpConnectorConfig.DefaultSignatureHeader,
            SignatureAlgorithm = config.TryGetValue(HttpConnectorConfig.WebhookSignatureAlgorithm, out var sa)
                ? sa : HttpConnectorConfig.DefaultSignatureAlgorithm,
            ValidateTimestamp = config.TryGetValue(HttpConnectorConfig.WebhookValidateTimestamp, out var vt)
                && bool.TryParse(vt, out var validate) && validate,
            TimestampHeader = config.TryGetValue(HttpConnectorConfig.WebhookTimestampHeader, out var th)
                ? th : HttpConnectorConfig.DefaultTimestampHeader,
            TimestampToleranceMs = config.TryGetValue(HttpConnectorConfig.WebhookTimestampToleranceMs, out var ttm)
                && long.TryParse(ttm, out var tolerance) ? tolerance : HttpConnectorConfig.DefaultTimestampToleranceMs,
            EventChannel = channel
        };

        if (!_registrations.TryAdd(name, registration))
        {
            throw new InvalidOperationException($"Webhook '{name}' is already registered.");
        }

        _pathToName[registration.Path] = name;

        return channel.Reader;
    }

    /// <summary>
    /// Unregister a webhook endpoint.
    /// </summary>
    public void Unregister(string name)
    {
        if (_registrations.TryRemove(name, out var registration))
        {
            _pathToName.TryRemove(registration.Path, out _);
            registration.EventChannel.Writer.Complete();
        }
    }

    /// <summary>
    /// Get the channel reader for a registered webhook.
    /// </summary>
    public ChannelReader<WebhookEvent>? GetReader(string name)
    {
        return _registrations.TryGetValue(name, out var reg) ? reg.EventChannel.Reader : null;
    }

    /// <summary>
    /// Check if a webhook is registered.
    /// </summary>
    public bool IsRegistered(string name) => _registrations.ContainsKey(name);

    /// <summary>
    /// Get all registered webhook paths.
    /// </summary>
    public IReadOnlyDictionary<string, string> GetRegisteredPaths()
    {
        return _pathToName.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    /// <summary>
    /// Find connector name by webhook path.
    /// </summary>
    public string? FindConnectorByPath(string path)
    {
        var normalized = NormalizePath(path);
        return _pathToName.TryGetValue(normalized, out var name) ? name : null;
    }

    /// <summary>
    /// Validate and enqueue a webhook event.
    /// </summary>
    /// <param name="name">Connector name.</param>
    /// <param name="body">Request body bytes.</param>
    /// <param name="headers">Request headers.</param>
    /// <returns>Validation result indicating success or failure reason.</returns>
    public async Task<WebhookValidationResult> ValidateAndEnqueueAsync(
        string name,
        byte[] body,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default)
    {
        if (!_registrations.TryGetValue(name, out var registration))
        {
            return WebhookValidationResult.NotFound;
        }

        // Validate signature if secret is configured
        if (!string.IsNullOrEmpty(registration.Secret))
        {
            if (!headers.TryGetValue(registration.SignatureHeader, out var signature)
                && !headers.TryGetValue(registration.SignatureHeader.ToLowerInvariant(), out signature))
            {
                return WebhookValidationResult.MissingSignature;
            }

            if (!SignatureValidator.ValidateSignature(body, signature, registration.Secret, registration.SignatureAlgorithm))
            {
                return WebhookValidationResult.InvalidSignature;
            }
        }

        // Validate timestamp if enabled
        if (registration.ValidateTimestamp)
        {
            headers.TryGetValue(registration.TimestampHeader, out var timestamp);
            if (timestamp == null)
            {
                headers.TryGetValue(registration.TimestampHeader.ToLowerInvariant(), out timestamp);
            }

            if (!SignatureValidator.ValidateTimestamp(timestamp, registration.TimestampToleranceMs))
            {
                return WebhookValidationResult.InvalidTimestamp;
            }
        }

        // Create and enqueue event
        var webhookEvent = new WebhookEvent(
            name,
            body,
            headers,
            DateTimeOffset.UtcNow);

        try
        {
            await registration.EventChannel.Writer.WriteAsync(webhookEvent, cancellationToken);
            return WebhookValidationResult.Success;
        }
        catch (ChannelClosedException)
        {
            return WebhookValidationResult.ChannelClosed;
        }
    }

    private static string NormalizePath(string path)
    {
        path = path.Trim();
        if (!path.StartsWith('/'))
            path = "/" + path;
        return path.TrimEnd('/').ToLowerInvariant();
    }
}

/// <summary>
/// Result of webhook validation and enqueue operation.
/// </summary>
public enum WebhookValidationResult
{
    Success,
    NotFound,
    MissingSignature,
    InvalidSignature,
    InvalidTimestamp,
    ChannelClosed
}
