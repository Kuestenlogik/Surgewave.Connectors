using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.HomeGraphService.v1;
using Google.Apis.HomeGraphService.v1.Data;
using Google.Apis.Services;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Home;

/// <summary>
/// Task that monitors Google Home smart home devices.
/// </summary>
public sealed class GoogleHomeSourceTask : SourceTask
{
    private HomeGraphServiceService? _service;
    private string _agentUserId = null!;
    private string _topic = null!;
    private int _pollIntervalMs;
    private bool _includeLights;
    private bool _includeSwitches;
    private bool _includeThermostats;
    private bool _includeLocks;
    private bool _includeSensors;
    private bool _eventsOnly;
    private HashSet<string>? _filterDeviceIds;
    private DateTime _lastPoll = DateTime.MinValue;
    private Dictionary<string, string> _lastStates = new();
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _agentUserId = config[GoogleHomeConnectorConfig.AgentUserId];
        _topic = config[GoogleHomeConnectorConfig.Topic];
        _pollIntervalMs = int.Parse(config.TryGetValue(GoogleHomeConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : GoogleHomeConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeLights = (config.TryGetValue(GoogleHomeConnectorConfig.IncludeLights, out var includeLights) ? includeLights : "true") == "true";
        _includeSwitches = (config.TryGetValue(GoogleHomeConnectorConfig.IncludeSwitches, out var includeSwitches) ? includeSwitches : "true") == "true";
        _includeThermostats = (config.TryGetValue(GoogleHomeConnectorConfig.IncludeThermostats, out var includeThermostats) ? includeThermostats : "true") == "true";
        _includeLocks = (config.TryGetValue(GoogleHomeConnectorConfig.IncludeLocks, out var includeLocks) ? includeLocks : "true") == "true";
        _includeSensors = (config.TryGetValue(GoogleHomeConnectorConfig.IncludeSensors, out var includeSensors) ? includeSensors : "true") == "true";
        _eventsOnly = (config.TryGetValue(GoogleHomeConnectorConfig.EventsOnly, out var eventsOnly) ? eventsOnly : "true") == "true";

        var filterStr = config.TryGetValue(GoogleHomeConnectorConfig.FilterDeviceIds, out var filterDeviceIds) ? filterDeviceIds : "";
        if (!string.IsNullOrWhiteSpace(filterStr))
        {
            _filterDeviceIds = filterStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet();
        }

        // Initialize credentials
        GoogleCredential credential;
        var jsonCredentials = config.TryGetValue(GoogleHomeConnectorConfig.ServiceAccountJson, out var jsonCreds) ? jsonCreds : null;
        var fileCredentials = config.TryGetValue(GoogleHomeConnectorConfig.ServiceAccountFile, out var fileCreds) ? fileCreds : null;

        if (!string.IsNullOrWhiteSpace(jsonCredentials))
        {
            credential = GoogleCredential.FromJson(jsonCredentials)
                .CreateScoped("https://www.googleapis.com/auth/homegraph");
        }
        else
        {
            credential = GoogleCredential.FromFile(fileCredentials!)
                .CreateScoped("https://www.googleapis.com/auth/homegraph");
        }

        _service = new HomeGraphServiceService(new BaseClientService.Initializer
        {
            HttpClientInitializer = credential,
            ApplicationName = "Surgewave Connect"
        });
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
            // Query device states
            var queryRequest = new QueryRequest
            {
                AgentUserId = _agentUserId,
                Inputs = new List<QueryRequestInput>
                {
                    new()
                    {
                        Payload = new QueryRequestPayload
                        {
                            Devices = await GetDeviceListAsync(cancellationToken)
                        }
                    }
                }
            };

            var queryResponse = await _service!.Devices.Query(queryRequest).ExecuteAsync(cancellationToken);
            if (queryResponse?.Payload?.Devices == null) return [];

            foreach (var kvp in queryResponse.Payload.Devices)
            {
                var deviceId = kvp.Key;
                var deviceState = kvp.Value as IDictionary<string, object>;

                if (_filterDeviceIds != null && !_filterDeviceIds.Contains(deviceId))
                    continue;

                var stateJson = JsonSerializer.Serialize(deviceState);
                if (_eventsOnly && _lastStates.TryGetValue(deviceId, out var lastState) && lastState == stateJson)
                    continue;

                _lastStates[deviceId] = stateJson;

                // Get device metadata for type info
                var deviceType = await GetDeviceTypeAsync(deviceId, cancellationToken);
                if (!ShouldIncludeDeviceType(deviceType))
                    continue;

                records.Add(CreateDeviceRecord(deviceId, deviceType, deviceState));
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private async Task<List<AgentDeviceId>> GetDeviceListAsync(CancellationToken ct)
    {
        var devices = new List<AgentDeviceId>();

        try
        {
            var syncRequest = new SyncRequest { AgentUserId = _agentUserId };
            var syncResponse = await _service!.Devices.Sync(syncRequest).ExecuteAsync(ct);

            if (syncResponse?.Payload?.Devices != null)
            {
                foreach (var device in syncResponse.Payload.Devices)
                {
                    devices.Add(new AgentDeviceId { Id = device.Id });
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return devices;
    }

    private async Task<string> GetDeviceTypeAsync(string deviceId, CancellationToken ct)
    {
        try
        {
            var syncRequest = new SyncRequest { AgentUserId = _agentUserId };
            var syncResponse = await _service!.Devices.Sync(syncRequest).ExecuteAsync(ct);

            var device = syncResponse?.Payload?.Devices?.FirstOrDefault(d => d.Id == deviceId);
            return device?.Type ?? "unknown";
        }
        catch (Exception)
        {
            return "unknown";
        }
    }

    private bool ShouldIncludeDeviceType(string deviceType)
    {
        return deviceType switch
        {
            "action.devices.types.LIGHT" => _includeLights,
            "action.devices.types.SWITCH" => _includeSwitches,
            "action.devices.types.OUTLET" => _includeSwitches,
            "action.devices.types.THERMOSTAT" => _includeThermostats,
            "action.devices.types.LOCK" => _includeLocks,
            "action.devices.types.SENSOR" => _includeSensors,
            _ => true
        };
    }

    private SourceRecord CreateDeviceRecord(string deviceId, string deviceType, IDictionary<string, object>? state)
    {
        var payload = new
        {
            type = "device",
            deviceId,
            deviceType,
            state,
            timestamp = DateTime.UtcNow
        };

        var msgId = Interlocked.Increment(ref _messageId);
        var shortType = deviceType.Replace("action.devices.types.", "").ToLowerInvariant();

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "google-home", ["type"] = shortType },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["device_id"] = deviceId
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"google-home:{deviceId}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["google.type"] = Encoding.UTF8.GetBytes(shortType),
                ["google.device.id"] = Encoding.UTF8.GetBytes(deviceId)
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
            _service?.Dispose();
        }
        base.Dispose(disposing);
    }
}
