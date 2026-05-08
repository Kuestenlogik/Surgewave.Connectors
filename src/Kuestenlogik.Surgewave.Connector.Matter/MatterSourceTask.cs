using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Matter;

/// <summary>
/// Task that monitors Matter devices via a Matter controller.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Interface used for extensibility")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "URL strings are simpler for REST API calls")]
public sealed class MatterSourceTask : SourceTask
{
    private HttpClient? _httpClient;
    private string _controllerUrl = null!;
    private string _topic = null!;
    private int _pollIntervalMs;
    private bool _includeLighting;
    private bool _includeSensors;
    private bool _includeSwitches;
    private bool _includeThermostat;
    private bool _includeDoorLock;
    private bool _eventsOnly;
    private HashSet<string>? _filterNodeIds;
    private DateTime _lastPoll = DateTime.MinValue;
    private Dictionary<string, string> _lastStates = new();
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _controllerUrl = config[MatterConnectorConfig.ControllerUrl].TrimEnd('/');
        _topic = config[MatterConnectorConfig.Topic];
        _pollIntervalMs = int.Parse(config.TryGetValue(MatterConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : MatterConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeLighting = (config.TryGetValue(MatterConnectorConfig.IncludeLighting, out var includeLighting) ? includeLighting : "true") == "true";
        _includeSensors = (config.TryGetValue(MatterConnectorConfig.IncludeSensors, out var includeSensors) ? includeSensors : "true") == "true";
        _includeSwitches = (config.TryGetValue(MatterConnectorConfig.IncludeSwitches, out var includeSwitches) ? includeSwitches : "true") == "true";
        _includeThermostat = (config.TryGetValue(MatterConnectorConfig.IncludeThermostat, out var includeThermostat) ? includeThermostat : "true") == "true";
        _includeDoorLock = (config.TryGetValue(MatterConnectorConfig.IncludeDoorLock, out var includeDoorLock) ? includeDoorLock : "true") == "true";
        _eventsOnly = (config.TryGetValue(MatterConnectorConfig.EventsOnly, out var eventsOnly) ? eventsOnly : "true") == "true";

        var filterStr = config.TryGetValue(MatterConnectorConfig.FilterNodeIds, out var filterNodeIds) ? filterNodeIds : "";
        if (!string.IsNullOrWhiteSpace(filterStr))
        {
            _filterNodeIds = filterStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet();
        }

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        var apiKey = config.TryGetValue(MatterConnectorConfig.ApiKey, out var apiKeyVal) ? apiKeyVal : null;
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        }
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
            // Get all nodes from the Matter controller
            var response = await _httpClient!.GetAsync($"{_controllerUrl}/api/nodes", cancellationToken);
            if (!response.IsSuccessStatusCode) return [];

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(json);

            if (!doc.RootElement.TryGetProperty("nodes", out var nodesArray))
                return [];

            foreach (var node in nodesArray.EnumerateArray())
            {
                var nodeId = node.GetProperty("node_id").GetString() ?? "";
                if (_filterNodeIds != null && !_filterNodeIds.Contains(nodeId))
                    continue;

                var deviceType = GetDeviceType(node);
                if (!ShouldIncludeDeviceType(deviceType))
                    continue;

                var stateJson = JsonSerializer.Serialize(node);
                var stateKey = $"{nodeId}";

                if (_eventsOnly && _lastStates.TryGetValue(stateKey, out var lastState) && lastState == stateJson)
                    continue;

                _lastStates[stateKey] = stateJson;
                records.Add(CreateDeviceRecord(node, nodeId, deviceType));
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private static string GetDeviceType(JsonElement node)
    {
        if (node.TryGetProperty("device_type", out var dtProp))
            return dtProp.GetString()?.ToLowerInvariant() ?? "unknown";

        // Try to infer from clusters
        if (node.TryGetProperty("clusters", out var clusters))
        {
            var clusterList = clusters.EnumerateArray().Select(c => c.GetString() ?? "").ToList();
            if (clusterList.Contains("OnOff") && clusterList.Contains("LevelControl"))
                return "light";
            if (clusterList.Contains("OnOff"))
                return "switch";
            if (clusterList.Contains("Thermostat"))
                return "thermostat";
            if (clusterList.Contains("DoorLock"))
                return "doorlock";
            if (clusterList.Contains("TemperatureMeasurement") || clusterList.Contains("OccupancySensing"))
                return "sensor";
        }

        return "unknown";
    }

    private bool ShouldIncludeDeviceType(string deviceType)
    {
        return deviceType switch
        {
            "light" or "dimmable_light" or "color_light" => _includeLighting,
            "sensor" or "temperature_sensor" or "occupancy_sensor" or "contact_sensor" => _includeSensors,
            "switch" or "on_off_switch" => _includeSwitches,
            "thermostat" => _includeThermostat,
            "doorlock" or "door_lock" => _includeDoorLock,
            _ => true
        };
    }

    private SourceRecord CreateDeviceRecord(JsonElement node, string nodeId, string deviceType)
    {
        var name = node.TryGetProperty("name", out var nameProp) ? nameProp.GetString() : null;
        var vendor = node.TryGetProperty("vendor_name", out var vendorProp) ? vendorProp.GetString() : null;
        var product = node.TryGetProperty("product_name", out var productProp) ? productProp.GetString() : null;

        // Extract relevant state based on device type
        object? state = null;
        if (node.TryGetProperty("attributes", out var attrs))
        {
            state = deviceType switch
            {
                "light" => ExtractLightState(attrs),
                "switch" => ExtractSwitchState(attrs),
                "thermostat" => ExtractThermostatState(attrs),
                "doorlock" or "door_lock" => ExtractDoorLockState(attrs),
                "sensor" => ExtractSensorState(attrs),
                _ => attrs
            };
        }

        var payload = new
        {
            type = "device",
            deviceType,
            nodeId,
            name,
            vendor,
            product,
            state,
            timestamp = DateTime.UtcNow
        };

        var msgId = Interlocked.Increment(ref _messageId);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "matter", ["type"] = deviceType },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["node_id"] = nodeId
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"matter:{nodeId}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["matter.type"] = Encoding.UTF8.GetBytes(deviceType),
                ["matter.node.id"] = Encoding.UTF8.GetBytes(nodeId),
                ["matter.name"] = Encoding.UTF8.GetBytes(name ?? "unknown")
            }
        };
    }

    private static object ExtractLightState(JsonElement attrs)
    {
        return new
        {
            on = attrs.TryGetProperty("OnOff", out var onOff) && onOff.TryGetProperty("on_off", out var on) && on.GetBoolean(),
            brightness = attrs.TryGetProperty("LevelControl", out var level) && level.TryGetProperty("current_level", out var br) ? br.GetInt32() : (int?)null,
            colorTemperature = attrs.TryGetProperty("ColorControl", out var color) && color.TryGetProperty("color_temperature_mireds", out var ct) ? ct.GetInt32() : (int?)null,
            hue = attrs.TryGetProperty("ColorControl", out var color2) && color2.TryGetProperty("current_hue", out var h) ? h.GetInt32() : (int?)null,
            saturation = attrs.TryGetProperty("ColorControl", out var color3) && color3.TryGetProperty("current_saturation", out var s) ? s.GetInt32() : (int?)null
        };
    }

    private static object ExtractSwitchState(JsonElement attrs)
    {
        return new
        {
            on = attrs.TryGetProperty("OnOff", out var onOff) && onOff.TryGetProperty("on_off", out var on) && on.GetBoolean()
        };
    }

    private static object ExtractThermostatState(JsonElement attrs)
    {
        if (!attrs.TryGetProperty("Thermostat", out var therm))
            return new { };

        return new
        {
            localTemperature = therm.TryGetProperty("local_temperature", out var lt) ? lt.GetInt32() / 100.0 : (double?)null,
            occupiedHeatingSetpoint = therm.TryGetProperty("occupied_heating_setpoint", out var heat) ? heat.GetInt32() / 100.0 : (double?)null,
            occupiedCoolingSetpoint = therm.TryGetProperty("occupied_cooling_setpoint", out var cool) ? cool.GetInt32() / 100.0 : (double?)null,
            systemMode = therm.TryGetProperty("system_mode", out var mode) ? mode.GetInt32() : (int?)null
        };
    }

    private static object ExtractDoorLockState(JsonElement attrs)
    {
        if (!attrs.TryGetProperty("DoorLock", out var doorLock))
            return new { };

        return new
        {
            lockState = doorLock.TryGetProperty("lock_state", out var ls) ? ls.GetInt32() : (int?)null,
            lockType = doorLock.TryGetProperty("lock_type", out var lt) ? lt.GetInt32() : (int?)null,
            actuatorEnabled = doorLock.TryGetProperty("actuator_enabled", out var ae) && ae.GetBoolean()
        };
    }

    private static object ExtractSensorState(JsonElement attrs)
    {
        var state = new Dictionary<string, object?>();

        if (attrs.TryGetProperty("TemperatureMeasurement", out var temp) &&
            temp.TryGetProperty("measured_value", out var tempVal))
        {
            state["temperature"] = tempVal.GetInt32() / 100.0;
        }

        if (attrs.TryGetProperty("RelativeHumidityMeasurement", out var hum) &&
            hum.TryGetProperty("measured_value", out var humVal))
        {
            state["humidity"] = humVal.GetInt32() / 100.0;
        }

        if (attrs.TryGetProperty("OccupancySensing", out var occ) &&
            occ.TryGetProperty("occupancy", out var occVal))
        {
            state["occupied"] = occVal.GetInt32() > 0;
        }

        if (attrs.TryGetProperty("BooleanState", out var boolState) &&
            boolState.TryGetProperty("state_value", out var stateVal))
        {
            state["contact"] = stateVal.GetBoolean();
        }

        return state;
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
