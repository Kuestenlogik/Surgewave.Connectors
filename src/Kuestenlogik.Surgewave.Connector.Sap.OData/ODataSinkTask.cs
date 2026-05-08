using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Simple.OData.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData;

/// <summary>
/// Task that writes entities to SAP OData services.
/// </summary>
[SuppressMessage("Performance", "CA1823:Avoid unused private fields", Justification = "Fields may be used conditionally")]
[SuppressMessage("Performance", "CA1854:Prefer Dictionary.TryGetValue", Justification = "Code clarity for OData operations")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "ODataClient interface used for flexibility")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
[SuppressMessage("Security", "CA5400:Ensure HttpClient certificate revocation list check is not disabled", Justification = "Certificate validation is intentionally configurable for SAP environments")]
public sealed class ODataSinkTask : SinkTask
{
    private ODataClient? _client;
    private HttpClient? _httpClient;
    private string _entitySet = null!;
    private string _writeMode = null!;
    private string[]? _keyFields;
    private int _batchSize;
    private bool _useBatch;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _entitySet = config[ODataConnectorConfig.TargetEntitySet];
        _writeMode = config.GetValueOrDefault(ODataConnectorConfig.WriteMode,
            ODataConnectorConfig.DefaultWriteMode)!.ToLowerInvariant();
        _batchSize = int.Parse(config.GetValueOrDefault(ODataConnectorConfig.BatchSize,
            ODataConnectorConfig.DefaultBatchSize.ToString())!);
        _useBatch = config.GetValueOrDefault(ODataConnectorConfig.UseBatch, "true") == "true";

        var keyFieldsStr = config.GetValueOrDefault(ODataConnectorConfig.KeyFields, "");
        if (!string.IsNullOrWhiteSpace(keyFieldsStr))
        {
            _keyFields = keyFieldsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _httpClient = CreateHttpClient(config);
        var settings = new ODataClientSettings(_httpClient);
        _client = new ODataClient(settings);
    }

    private HttpClient CreateHttpClient(IDictionary<string, string> config)
    {
        var serviceUrl = config[ODataConnectorConfig.ServiceUrl];
        var authType = config.GetValueOrDefault(ODataConnectorConfig.AuthType,
            ODataConnectorConfig.DefaultAuthType)!.ToLowerInvariant();
        var ignoreCertErrors = config.GetValueOrDefault(ODataConnectorConfig.IgnoreCertificateErrors, "false") == "true";
        var timeoutSeconds = int.Parse(config.GetValueOrDefault(ODataConnectorConfig.TimeoutSeconds,
            ODataConnectorConfig.DefaultTimeoutSeconds.ToString())!);

        var handler = new HttpClientHandler();
        if (ignoreCertErrors)
        {
            handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
        }

        var client = new HttpClient(handler)
        {
            BaseAddress = new Uri(serviceUrl),
            Timeout = TimeSpan.FromSeconds(timeoutSeconds)
        };

        // Set authentication
        if (authType == "basic")
        {
            var username = config.GetValueOrDefault(ODataConnectorConfig.Username, "")!;
            var password = config.GetValueOrDefault(ODataConnectorConfig.Password, "")!;
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // SAP-specific headers
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        client.DefaultRequestHeaders.Add("sap-client", config.GetValueOrDefault("sap.client", "100"));

        return client;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var batch = new List<Dictionary<string, object?>>();

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                var dataElement = root.TryGetProperty("data", out var data) ? data : root;

                var entity = new Dictionary<string, object?>();
                foreach (var prop in dataElement.EnumerateObject())
                {
                    entity[prop.Name] = ConvertJsonValue(prop.Value);
                }

                if (entity.Count > 0)
                {
                    batch.Add(entity);
                }

                if (batch.Count >= _batchSize)
                {
                    await FlushBatchAsync(batch, cancellationToken);
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
            await FlushBatchAsync(batch, cancellationToken);
        }
    }

    private object? ConvertJsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private async Task FlushBatchAsync(List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        if (batch.Count == 0) return;

        try
        {
            if (_useBatch && batch.Count > 1)
            {
                await ExecuteBatchAsync(batch, ct);
            }
            else
            {
                foreach (var entity in batch)
                {
                    await ExecuteSingleAsync(entity, ct);
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private async Task ExecuteBatchAsync(List<Dictionary<string, object?>> batch, CancellationToken ct)
    {
        var oDataBatch = new ODataBatch(_client!);

        foreach (var entity in batch)
        {
            switch (_writeMode)
            {
                case "create":
                    oDataBatch += c => c.For(_entitySet).Set(entity).InsertEntryAsync(ct);
                    break;

                case "update":
                case "patch":
                    if (_keyFields != null && HasAllKeys(entity))
                    {
                        var keys = GetKeyValues(entity);
                        oDataBatch += c => c.For(_entitySet).Key(keys).Set(entity).UpdateEntryAsync(ct);
                    }
                    break;

                case "delete":
                    if (_keyFields != null && HasAllKeys(entity))
                    {
                        var keys = GetKeyValues(entity);
                        oDataBatch += c => c.For(_entitySet).Key(keys).DeleteEntryAsync(ct);
                    }
                    break;
            }
        }

        await oDataBatch.ExecuteAsync(ct);
    }

    private async Task ExecuteSingleAsync(Dictionary<string, object?> entity, CancellationToken ct)
    {
        switch (_writeMode)
        {
            case "create":
                await _client!.For(_entitySet).Set(entity).InsertEntryAsync(ct);
                break;

            case "update":
            case "patch":
                if (_keyFields != null && HasAllKeys(entity))
                {
                    var keys = GetKeyValues(entity);
                    await _client!.For(_entitySet).Key(keys).Set(entity).UpdateEntryAsync(ct);
                }
                break;

            case "delete":
                if (_keyFields != null && HasAllKeys(entity))
                {
                    var keys = GetKeyValues(entity);
                    await _client!.For(_entitySet).Key(keys).DeleteEntryAsync(ct);
                }
                break;
        }
    }

    private bool HasAllKeys(Dictionary<string, object?> entity)
    {
        if (_keyFields == null) return false;
        return _keyFields.All(k => entity.ContainsKey(k) && entity[k] != null);
    }

    private IDictionary<string, object> GetKeyValues(Dictionary<string, object?> entity)
    {
        var keys = new Dictionary<string, object>();
        foreach (var key in _keyFields!)
        {
            if (entity.TryGetValue(key, out var value) && value != null)
            {
                keys[key] = value;
            }
        }
        return keys;
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
