using System.Collections.Frozen;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Alexa;

/// <summary>
/// Task that monitors Alexa smart home devices.
/// </summary>
public sealed class AlexaSourceTask : SourceTask
{
    private HttpClient? _httpClient;
    private string _clientId = null!;
    private string _clientSecret = null!;
    private string _refreshToken = null!;
    private string _region = null!;
    private string _topic = null!;
    private int _pollIntervalMs;
    private bool _includeLights;
    private bool _includeSwitches;
    private bool _includeThermostats;
    private bool _includeLocks;
    private bool _includeSensors;
    private bool _eventsOnly;
    private HashSet<string>? _filterEndpointIds;
    private DateTime _lastPoll = DateTime.MinValue;
    private Dictionary<string, string> _lastStates = new();
    private string? _accessToken;
    private DateTime _tokenExpiry = DateTime.MinValue;
    private long _messageId;

    private static readonly FrozenDictionary<string, string> RegionEndpoints = new Dictionary<string, string>
    {
        ["NA"] = "https://api.amazonalexa.com",
        ["EU"] = "https://api.eu.amazonalexa.com",
        ["FE"] = "https://api.fe.amazonalexa.com"
    }.ToFrozenDictionary();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _clientId = config[AlexaConnectorConfig.ClientId];
        _clientSecret = config[AlexaConnectorConfig.ClientSecret];
        _refreshToken = config[AlexaConnectorConfig.RefreshToken];
        _region = config.TryGetValue(AlexaConnectorConfig.Region, out var region) ? region : AlexaConnectorConfig.DefaultRegion;
        _topic = config[AlexaConnectorConfig.Topic];
        _pollIntervalMs = int.Parse(config.TryGetValue(AlexaConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : AlexaConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeLights = (config.TryGetValue(AlexaConnectorConfig.IncludeLights, out var lights) ? lights : "true") == "true";
        _includeSwitches = (config.TryGetValue(AlexaConnectorConfig.IncludeSwitches, out var switches) ? switches : "true") == "true";
        _includeThermostats = (config.TryGetValue(AlexaConnectorConfig.IncludeThermostats, out var thermostats) ? thermostats : "true") == "true";
        _includeLocks = (config.TryGetValue(AlexaConnectorConfig.IncludeLocks, out var locks) ? locks : "true") == "true";
        _includeSensors = (config.TryGetValue(AlexaConnectorConfig.IncludeSensors, out var sensors) ? sensors : "true") == "true";
        _eventsOnly = (config.TryGetValue(AlexaConnectorConfig.EventsOnly, out var eventsOnly) ? eventsOnly : "true") == "true";

        var filterStr = config.TryGetValue(AlexaConnectorConfig.FilterEndpointIds, out var filter) ? filter : "";
        if (!string.IsNullOrWhiteSpace(filterStr))
        {
            _filterEndpointIds = filterStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet();
        }

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
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
            await EnsureAccessTokenAsync(cancellationToken);
            if (string.IsNullOrEmpty(_accessToken)) return [];

            var baseUrl = RegionEndpoints.TryGetValue(_region, out var url) ? url : RegionEndpoints["NA"];

            // Get all endpoints (devices)
            using var endpointsRequest = new HttpRequestMessage(HttpMethod.Get, $"{baseUrl}/v1/endpoints");
            endpointsRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            using var endpointsResponse = await _httpClient!.SendAsync(endpointsRequest, cancellationToken);
            if (!endpointsResponse.IsSuccessStatusCode) return [];

            var endpointsJson = await endpointsResponse.Content.ReadAsStringAsync(cancellationToken);
            using var endpointsDoc = JsonDocument.Parse(endpointsJson);

            if (!endpointsDoc.RootElement.TryGetProperty("endpoints", out var endpointsArray))
                return [];

            foreach (var endpoint in endpointsArray.EnumerateArray())
            {
                var endpointId = endpoint.GetProperty("endpointId").GetString() ?? "";
                if (_filterEndpointIds != null && !_filterEndpointIds.Contains(endpointId))
                    continue;

                var displayCategories = GetCategories(endpoint);
                if (!ShouldIncludeCategories(displayCategories))
                    continue;

                // Get state report for this endpoint
                var state = await GetEndpointStateAsync(baseUrl, endpointId, cancellationToken);
                var stateJson = JsonSerializer.Serialize(state);

                if (_eventsOnly && _lastStates.TryGetValue(endpointId, out var lastState) && lastState == stateJson)
                    continue;

                _lastStates[endpointId] = stateJson;
                records.Add(CreateDeviceRecord(endpoint, endpointId, displayCategories, state));
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private async Task EnsureAccessTokenAsync(CancellationToken ct)
    {
        if (_accessToken != null && DateTime.UtcNow < _tokenExpiry)
            return;

        try
        {
            using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, "https://api.amazon.com/auth/o2/token");
            using var formContent = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "refresh_token",
                ["refresh_token"] = _refreshToken,
                ["client_id"] = _clientId,
                ["client_secret"] = _clientSecret
            });
            tokenRequest.Content = formContent;

            using var response = await _httpClient!.SendAsync(tokenRequest, ct);
            if (!response.IsSuccessStatusCode) return;

            var json = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(json);

            _accessToken = doc.RootElement.GetProperty("access_token").GetString();
            var expiresIn = doc.RootElement.GetProperty("expires_in").GetInt32();
            _tokenExpiry = DateTime.UtcNow.AddSeconds(expiresIn - 60);
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private async Task<Dictionary<string, object?>> GetEndpointStateAsync(string baseUrl, string endpointId, CancellationToken ct)
    {
        var state = new Dictionary<string, object?>();

        try
        {
            using var stateRequest = new HttpRequestMessage(HttpMethod.Get, $"{baseUrl}/v2/endpoints/{endpointId}/state");
            stateRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            using var response = await _httpClient!.SendAsync(stateRequest, ct);
            if (!response.IsSuccessStatusCode) return state;

            var json = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(json);

            if (!doc.RootElement.TryGetProperty("properties", out var properties))
                return state;

            foreach (var prop in properties.EnumerateArray())
            {
                var ns = prop.TryGetProperty("namespace", out var nsProp) ? nsProp.GetString() : "";
                var name = prop.TryGetProperty("name", out var nameProp) ? nameProp.GetString() : "";
                var value = prop.TryGetProperty("value", out var valueProp) ? valueProp : default;

                var key = $"{ns}.{name}";
                state[key] = value.ValueKind switch
                {
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    JsonValueKind.String => value.GetString(),
                    JsonValueKind.Number => value.TryGetInt32(out var i) ? i : value.GetDouble(),
                    _ => value.ToString()
                };
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return state;
    }

    private static List<string> GetCategories(JsonElement endpoint)
    {
        var categories = new List<string>();
        if (endpoint.TryGetProperty("displayCategories", out var cats))
        {
            foreach (var cat in cats.EnumerateArray())
            {
                categories.Add(cat.GetString()?.ToUpperInvariant() ?? "");
            }
        }
        return categories;
    }

    private bool ShouldIncludeCategories(List<string> categories)
    {
        foreach (var cat in categories)
        {
            switch (cat)
            {
                case "LIGHT":
                case "SMARTLIGHT":
                    if (_includeLights) return true;
                    break;
                case "SWITCH":
                case "SMARTPLUG":
                    if (_includeSwitches) return true;
                    break;
                case "THERMOSTAT":
                    if (_includeThermostats) return true;
                    break;
                case "LOCK":
                case "SMARTLOCK":
                    if (_includeLocks) return true;
                    break;
                case "TEMPERATURESENSOR":
                case "MOTIONSENSOR":
                case "CONTACTSENSOR":
                    if (_includeSensors) return true;
                    break;
                default:
                    return true;
            }
        }
        return categories.Count == 0;
    }

    private SourceRecord CreateDeviceRecord(JsonElement endpoint, string endpointId, List<string> categories, Dictionary<string, object?> state)
    {
        var friendlyName = endpoint.TryGetProperty("friendlyName", out var fnProp) ? fnProp.GetString() : null;
        var description = endpoint.TryGetProperty("description", out var descProp) ? descProp.GetString() : null;
        var manufacturerName = endpoint.TryGetProperty("manufacturerName", out var mnProp) ? mnProp.GetString() : null;

        var payload = new
        {
            type = "device",
            endpointId,
            friendlyName,
            description,
            manufacturerName,
            categories,
            state,
            timestamp = DateTime.UtcNow
        };

        var msgId = Interlocked.Increment(ref _messageId);
        var primaryCategory = categories.FirstOrDefault() ?? "UNKNOWN";

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "alexa", ["category"] = primaryCategory },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["endpoint_id"] = endpointId
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"alexa:{endpointId}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["alexa.category"] = Encoding.UTF8.GetBytes(primaryCategory),
                ["alexa.endpoint.id"] = Encoding.UTF8.GetBytes(endpointId),
                ["alexa.name"] = Encoding.UTF8.GetBytes(friendlyName ?? "unknown")
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
