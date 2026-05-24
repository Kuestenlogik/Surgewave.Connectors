namespace Kuestenlogik.Surgewave.Connector.Http;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task that sends records to an HTTP endpoint with authentication support.
/// Supports single and batch modes with configurable retry logic.
/// </summary>
public sealed class HttpSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private Uri _url = null!;
    private string _httpMethod = "POST";
    private string _contentType = HttpConnectorConfig.DefaultContentType;
    private string _batchMode = HttpConnectorConfig.BatchModeSingle;
    private int _batchSize = HttpConnectorConfig.DefaultBatchSize;
    private int _retryMax = HttpConnectorConfig.DefaultRetryMax;
    private long _retryBackoffMs = HttpConnectorConfig.DefaultRetryBackoffMs;
    private HttpClient? _httpClient;
    private IAuthenticationProvider _authProvider = NoAuthProvider.Instance;
    private readonly List<SinkRecord> _buffer = [];

    public override void Start(IDictionary<string, string> config)
    {
        _url = new Uri(config[HttpConnectorConfig.Url]);

        _httpMethod = config.TryGetValue(HttpConnectorConfig.Method, out var method)
            ? method : "POST";
        _contentType = config.TryGetValue(HttpConnectorConfig.ContentType, out var ct)
            ? ct : HttpConnectorConfig.DefaultContentType;
        _batchMode = config.TryGetValue(HttpConnectorConfig.BatchMode, out var bm)
            ? bm : HttpConnectorConfig.BatchModeSingle;
        _batchSize = config.TryGetValue(HttpConnectorConfig.BatchSize, out var bs)
            ? int.Parse(bs) : HttpConnectorConfig.DefaultBatchSize;
        _retryMax = config.TryGetValue(HttpConnectorConfig.RetryMax, out var rm)
            ? int.Parse(rm) : HttpConnectorConfig.DefaultRetryMax;
        _retryBackoffMs = config.TryGetValue(HttpConnectorConfig.RetryBackoffMs, out var rb)
            ? long.Parse(rb) : HttpConnectorConfig.DefaultRetryBackoffMs;

        // Create authentication provider
        _authProvider = AuthenticationProviderFactory.Create(config);

        _httpClient = new HttpClient();

        // Parse and apply static headers
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

    public override void Stop()
    {
        FlushBuffer();
        _httpClient?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _buffer.Clear();
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null || records.Count == 0)
            return;

        if (_batchMode == HttpConnectorConfig.BatchModeArray)
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
                await SendSingleAsync(record, cancellationToken);
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
        if (_buffer.Count == 0)
            return;

        await SendBatchAsync(_buffer, cancellationToken);
        _buffer.Clear();
    }

    private async Task SendSingleAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var body = record.Value;
        await SendWithRetryAsync(body, cancellationToken);
    }

    private async Task SendBatchAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Build JSON array from records
        var jsonElements = new List<JsonElement>();

        foreach (var record in records)
        {
            var content = Encoding.UTF8.GetString(record.Value);
            try
            {
                using var doc = JsonDocument.Parse(content);
                jsonElements.Add(doc.RootElement.Clone());
            }
            catch (JsonException)
            {
                // If not valid JSON, wrap as string
                using var stringDoc = JsonDocument.Parse($"\"{EscapeJson(content)}\"");
                jsonElements.Add(stringDoc.RootElement.Clone());
            }
        }

        var batchJson = JsonSerializer.Serialize(jsonElements);
        var body = Encoding.UTF8.GetBytes(batchJson);
        await SendWithRetryAsync(body, cancellationToken);
    }

    private static string EscapeJson(string str)
    {
        return str
            .Replace("\\", "\\\\")
            .Replace("\"", "\\\"")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r")
            .Replace("\t", "\\t");
    }

    private async Task SendWithRetryAsync(byte[] body, CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                using var request = new HttpRequestMessage(
                    _httpMethod.Equals("PUT", StringComparison.OrdinalIgnoreCase) ? HttpMethod.Put : HttpMethod.Post,
                    _url);

                request.Content = new ByteArrayContent(body);
                request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(_contentType);

                // Apply authentication (may add signature based on body)
                _authProvider.ApplyAuthentication(request, body);

                var response = await _httpClient!.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                return; // Success
            }
            catch (HttpRequestException) when (attempt < _retryMax)
            {
                attempt++;
                var backoff = (int)(_retryBackoffMs * Math.Pow(2, attempt - 1));
                await Task.Delay(backoff, cancellationToken);
            }
        }
    }
}
