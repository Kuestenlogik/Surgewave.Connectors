using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Simple.OData.Client;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData;

/// <summary>
/// Task that reads entities from SAP OData services.
/// </summary>
[SuppressMessage("Performance", "CA1823:Avoid unused private fields", Justification = "Fields may be used conditionally")]
[SuppressMessage("Performance", "CA1854:Prefer Dictionary.TryGetValue", Justification = "Code clarity for OData operations")]
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "ODataClient interface used for flexibility")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
[SuppressMessage("Security", "CA5400:Ensure HttpClient certificate revocation list check is not disabled", Justification = "Certificate validation is intentionally configurable for SAP environments")]
public sealed class ODataSourceTask : SourceTask
{
    private ODataClient? _client;
    private HttpClient? _httpClient;
    private string _topic = null!;
    private string _entitySet = null!;
    private string[]? _selectFields;
    private string? _filter;
    private string[]? _expandProperties;
    private string? _orderBy;
    private int _top;
    private string? _incrementalField;
    private int _pollIntervalMs;
    private bool _useDelta;
    private DateTime _lastPoll = DateTime.MinValue;
    private object? _lastIncrementalValue;
    private string? _deltaLink;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[ODataConnectorConfig.Topic];
        _entitySet = config[ODataConnectorConfig.EntitySet];

        var selectStr = config.GetValueOrDefault(ODataConnectorConfig.Select, "");
        if (!string.IsNullOrWhiteSpace(selectStr))
        {
            _selectFields = selectStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _filter = config.GetValueOrDefault(ODataConnectorConfig.Filter, null);

        var expandStr = config.GetValueOrDefault(ODataConnectorConfig.Expand, "");
        if (!string.IsNullOrWhiteSpace(expandStr))
        {
            _expandProperties = expandStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _orderBy = config.GetValueOrDefault(ODataConnectorConfig.OrderBy, null);
        _top = int.Parse(config.GetValueOrDefault(ODataConnectorConfig.Top,
            ODataConnectorConfig.DefaultTop.ToString())!);
        _incrementalField = config.GetValueOrDefault(ODataConnectorConfig.IncrementalField, null);
        _pollIntervalMs = int.Parse(config.GetValueOrDefault(ODataConnectorConfig.PollIntervalMs,
            ODataConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _useDelta = config.GetValueOrDefault(ODataConnectorConfig.DeltaLink, "false") == "true";

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
        switch (authType)
        {
            case "basic":
                var username = config.GetValueOrDefault(ODataConnectorConfig.Username, "")!;
                var password = config.GetValueOrDefault(ODataConnectorConfig.Password, "")!;
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
                break;

            case "oauth":
                // OAuth token would be fetched asynchronously
                // For simplicity, assume token is pre-configured or fetch synchronously
                break;
        }

        // SAP-specific headers
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        client.DefaultRequestHeaders.Add("sap-client", config.GetValueOrDefault("sap.client", "100"));

        return client;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            var command = _client!.For(_entitySet);

            // Apply select
            if (_selectFields != null && _selectFields.Length > 0)
            {
                command = command.Select(_selectFields);
            }

            // Apply expand
            if (_expandProperties != null)
            {
                foreach (var expand in _expandProperties)
                {
                    command = command.Expand(expand);
                }
            }

            // Apply filter
            var filter = BuildFilter();
            if (!string.IsNullOrEmpty(filter))
            {
                command = command.Filter(filter);
            }

            // Apply order
            if (!string.IsNullOrEmpty(_orderBy))
            {
                command = command.OrderBy(_orderBy);
            }

            // Apply top
            command = command.Top(_top);

            var entities = await command.FindEntriesAsync(cancellationToken);

            foreach (var entity in entities)
            {
                var record = CreateRecord(entity);
                records.Add(record);

                // Update incremental value
                if (!string.IsNullOrEmpty(_incrementalField) && entity.ContainsKey(_incrementalField))
                {
                    _lastIncrementalValue = entity[_incrementalField];
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private string? BuildFilter()
    {
        var filters = new List<string>();

        if (!string.IsNullOrEmpty(_filter))
        {
            filters.Add(_filter);
        }

        if (_lastIncrementalValue != null && !string.IsNullOrEmpty(_incrementalField))
        {
            var value = _lastIncrementalValue switch
            {
                DateTime dt => $"datetime'{dt:yyyy-MM-ddTHH:mm:ss}'",
                DateTimeOffset dto => $"datetimeoffset'{dto:yyyy-MM-ddTHH:mm:sszzz}'",
                string s => $"'{s}'",
                _ => _lastIncrementalValue.ToString()
            };
            filters.Add($"{_incrementalField} gt {value}");
        }

        return filters.Count > 0 ? string.Join(" and ", filters) : null;
    }

    private SourceRecord CreateRecord(IDictionary<string, object> entity)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        var payload = new Dictionary<string, object?>
        {
            ["entitySet"] = _entitySet,
            ["data"] = entity,
            ["timestamp"] = DateTime.UtcNow
        };

        // Use first property as key
        string? keyStr = null;
        if (entity.Count > 0)
        {
            var firstValue = entity.Values.FirstOrDefault();
            keyStr = firstValue?.ToString();
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "odata",
                ["entity_set"] = _entitySet
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["incremental_value"] = _lastIncrementalValue?.ToString() ?? ""
            },
            Topic = _topic,
            Key = keyStr != null ? Encoding.UTF8.GetBytes(keyStr) : null,
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["odata.entity_set"] = Encoding.UTF8.GetBytes(_entitySet)
            }
        };
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
