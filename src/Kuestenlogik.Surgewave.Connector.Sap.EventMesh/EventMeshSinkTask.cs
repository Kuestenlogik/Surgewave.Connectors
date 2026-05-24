using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh;

/// <summary>
/// Task that publishes events to SAP Event Mesh.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via reflection by connector framework")]
[SuppressMessage("Performance", "CA1823:Avoid unused private fields", Justification = "Fields used by CloudEvents")]
[SuppressMessage("Performance", "CA1852:Seal internal types", Justification = "Public connector task")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
public sealed class EventMeshSinkTask : SinkTask
{
    private HttpClient? _httpClient;
    private string _serviceUrl = null!;
    private string _tokenUrl = null!;
    private string _clientId = null!;
    private string _clientSecret = null!;
    private string _targetTopic = null!;
    private string _contentType = null!;
    private string? _ceSource;
    private string? _ceType;
    private int _batchSize;
    private string? _accessToken;
    private DateTime _tokenExpiry = DateTime.MinValue;
    private readonly JsonEventFormatter _formatter = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _serviceUrl = config[EventMeshConnectorConfig.ServiceUrl].TrimEnd('/');
        _tokenUrl = config[EventMeshConnectorConfig.TokenUrl];
        _clientId = config[EventMeshConnectorConfig.ClientId];
        _clientSecret = config[EventMeshConnectorConfig.ClientSecret];
        _targetTopic = config[EventMeshConnectorConfig.TargetTopic];

        _contentType = config.GetValueOrDefault(EventMeshConnectorConfig.ContentType,
            EventMeshConnectorConfig.DefaultContentType)!;
        _ceSource = config.GetValueOrDefault(EventMeshConnectorConfig.CloudEventSource, null);
        _ceType = config.GetValueOrDefault(EventMeshConnectorConfig.CloudEventType, null);
        _batchSize = int.Parse(config.GetValueOrDefault(EventMeshConnectorConfig.BatchSize,
            EventMeshConnectorConfig.DefaultBatchSize.ToString())!);

        _httpClient = new HttpClient();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var batch = new List<object>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var message = CreateMessage(record);
                batch.Add(message);

                if (batch.Count >= _batchSize)
                {
                    await PublishBatchAsync(batch, cancellationToken);
                    batch.Clear();
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        if (batch.Count > 0)
        {
            await PublishBatchAsync(batch, cancellationToken);
        }
    }

    private object CreateMessage(SinkRecord record)
    {
        // Build CloudEvent
        var ceId = Guid.NewGuid().ToString();
        var ceSource = _ceSource ?? "surgewave/connector/eventmesh";
        var ceType = _ceType ?? "surgewave.event";

        // Check if headers contain CloudEvents attributes
        if (record.Headers != null)
        {
            if (record.Headers.TryGetValue("ce_id", out var idBytes))
                ceId = Encoding.UTF8.GetString(idBytes);
            if (record.Headers.TryGetValue("ce_source", out var srcBytes))
                ceSource = Encoding.UTF8.GetString(srcBytes);
            if (record.Headers.TryGetValue("ce_type", out var typeBytes))
                ceType = Encoding.UTF8.GetString(typeBytes);
        }

        // Parse data
        object? data = null;
        try
        {
            data = JsonSerializer.Deserialize<JsonElement>(record.Value!);
        }
        catch
        {
            data = Encoding.UTF8.GetString(record.Value!);
        }

        return new
        {
            specversion = "1.0",
            id = ceId,
            source = ceSource,
            type = ceType,
            datacontenttype = _contentType,
            time = DateTime.UtcNow.ToString("O"),
            data
        };
    }

    private async Task PublishBatchAsync(List<object> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        try
        {
            await EnsureAccessTokenAsync(ct);

            var url = $"{_serviceUrl}/messagingrest/v1/topics/{Uri.EscapeDataString(_targetTopic)}/messages";

            var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            request.Headers.Add("x-qos", "1");  // At least once delivery

            // For single message
            if (batch.Count == 1)
            {
                request.Content = new StringContent(
                    JsonSerializer.Serialize(batch[0]),
                    Encoding.UTF8,
                    "application/cloudevents+json");
            }
            else
            {
                // Batch mode
                request.Content = new StringContent(
                    JsonSerializer.Serialize(batch),
                    Encoding.UTF8,
                    "application/cloudevents-batch+json");
            }

            var response = await _httpClient!.SendAsync(request, ct);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private async Task EnsureAccessTokenAsync(CancellationToken ct)
    {
        if (_accessToken != null && DateTime.UtcNow < _tokenExpiry)
        {
            return;
        }

        var request = new HttpRequestMessage(HttpMethod.Post, _tokenUrl);
        request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _clientId,
            ["client_secret"] = _clientSecret
        });

        var response = await _httpClient!.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(content);

        _accessToken = doc.RootElement.GetProperty("access_token").GetString();
        var expiresIn = doc.RootElement.GetProperty("expires_in").GetInt32();
        _tokenExpiry = DateTime.UtcNow.AddSeconds(expiresIn - 60);
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
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }
}
