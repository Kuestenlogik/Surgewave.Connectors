using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Q42.HueApi;
using Q42.HueApi.Interfaces;
using Q42.HueApi.Models;
using Q42.HueApi.Models.Groups;

namespace Kuestenlogik.Surgewave.Connector.Hue;

/// <summary>
/// Task that monitors Philips Hue bridge for state changes.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Interface used for extensibility and testability")]
public sealed class HueSourceTask : SourceTask
{
    private ILocalHueClient? _client;
    private string _topic = null!;
    private int _pollIntervalMs;
    private bool _includeLights;
    private bool _includeSensors;
    private bool _includeGroups;
    private bool _includeScenes;
    private bool _eventsOnly;
    private DateTime _lastPoll = DateTime.MinValue;
    private Dictionary<string, string> _lastLightStates = new();
    private Dictionary<string, string> _lastSensorStates = new();
    private Dictionary<string, string> _lastGroupStates = new();
    private Dictionary<string, string> _lastSceneStates = new();
    private long _messageId;
    [SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Test seam only: the caller owns and disposes the injected HttpClient")]
    private readonly HttpClient? _httpClient;

    public HueSourceTask()
    {
    }

    /// <summary>
    /// Test seam: talks to the bridge through a caller-supplied HttpClient, which the caller
    /// also owns and disposes.
    /// </summary>
    internal HueSourceTask(HttpClient httpClient) => _httpClient = httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var bridgeIp = config[HueConnectorConfig.BridgeIp];
        var appKey = config[HueConnectorConfig.AppKey];
        _topic = config[HueConnectorConfig.Topic];
        _pollIntervalMs = int.Parse(config.TryGetValue(HueConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : HueConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeLights = (config.TryGetValue(HueConnectorConfig.IncludeLights, out var includeLights) ? includeLights : "true") == "true";
        _includeSensors = (config.TryGetValue(HueConnectorConfig.IncludeSensors, out var includeSensors) ? includeSensors : "true") == "true";
        _includeGroups = (config.TryGetValue(HueConnectorConfig.IncludeGroups, out var includeGroups) ? includeGroups : "true") == "true";
        _includeScenes = (config.TryGetValue(HueConnectorConfig.IncludeScenes, out var includeScenes) ? includeScenes : "false") == "true";
        _eventsOnly = (config.TryGetValue(HueConnectorConfig.EventsOnly, out var eventsOnly) ? eventsOnly : "true") == "true";

        _client = _httpClient != null ? new LocalHueClient(bridgeIp, _httpClient) : new LocalHueClient(bridgeIp);
        _client.Initialize(appKey);
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
            if (_includeLights)
            {
                var lights = await _client!.GetLightsAsync();
                foreach (var light in lights)
                {
                    var state = JsonSerializer.Serialize(new
                    {
                        on = light.State.On,
                        brightness = light.State.Brightness,
                        hue = light.State.Hue,
                        saturation = light.State.Saturation,
                        colorTemperature = light.State.ColorTemperature,
                        reachable = light.State.IsReachable
                    });

                    if (!_eventsOnly || !_lastLightStates.TryGetValue(light.Id, out var lastState) || lastState != state)
                    {
                        records.Add(CreateLightRecord(light));
                        _lastLightStates[light.Id] = state;
                    }
                }
            }

            if (_includeSensors)
            {
                var sensors = await _client!.GetSensorsAsync();
                foreach (var sensor in sensors)
                {
                    var state = JsonSerializer.Serialize(sensor.State);

                    if (!_eventsOnly || !_lastSensorStates.TryGetValue(sensor.Id, out var lastState) || lastState != state)
                    {
                        records.Add(CreateSensorRecord(sensor));
                        _lastSensorStates[sensor.Id] = state;
                    }
                }
            }

            if (_includeGroups)
            {
                var groups = await _client!.GetGroupsAsync();
                foreach (var group in groups)
                {
                    var state = JsonSerializer.Serialize(new
                    {
                        allOn = group.State?.AllOn,
                        anyOn = group.State?.AnyOn
                    });

                    if (!_eventsOnly || !_lastGroupStates.TryGetValue(group.Id, out var lastState) || lastState != state)
                    {
                        records.Add(CreateGroupRecord(group));
                        _lastGroupStates[group.Id] = state;
                    }
                }
            }

            if (_includeScenes)
            {
                var scenes = await _client!.GetScenesAsync();
                foreach (var scene in scenes)
                {
                    var state = JsonSerializer.Serialize(new
                    {
                        name = scene.Name,
                        lights = scene.Lights,
                        lastUpdated = scene.LastUpdated
                    });

                    if (!_eventsOnly || !_lastSceneStates.TryGetValue(scene.Id, out var lastState) || lastState != state)
                    {
                        records.Add(CreateSceneRecord(scene));
                        _lastSceneStates[scene.Id] = state;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Bridge connectivity/auth failures must stay visible - the records collected
            // before the failure are still returned so partial progress is not lost.
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    private SourceRecord CreateLightRecord(Light light)
    {
        var payload = new
        {
            type = "light",
            id = light.Id,
            name = light.Name,
            model = light.ModelId,
            manufacturer = light.ManufacturerName,
            product = light.Name, // ProductName not available in this API version
            state = new
            {
                on = light.State.On,
                brightness = light.State.Brightness,
                hue = light.State.Hue,
                saturation = light.State.Saturation,
                colorTemperature = light.State.ColorTemperature,
                colorMode = light.State.ColorMode,
                reachable = light.State.IsReachable,
                effect = light.State.Effect,
                alert = light.State.Alert
            }
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "hue", ["type"] = "light" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["light_id"] = light.Id
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"light:{light.Id}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["hue.type"] = Encoding.UTF8.GetBytes("light"),
                ["hue.id"] = Encoding.UTF8.GetBytes(light.Id),
                ["hue.name"] = Encoding.UTF8.GetBytes(light.Name)
            }
        };
    }

    private SourceRecord CreateSensorRecord(Sensor sensor)
    {
        var payload = new
        {
            type = "sensor",
            id = sensor.Id,
            name = sensor.Name,
            sensorType = sensor.Type,
            model = sensor.ModelId,
            manufacturer = sensor.ManufacturerName,
            state = sensor.State,
            config = sensor.Config
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "hue", ["type"] = "sensor" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["sensor_id"] = sensor.Id
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"sensor:{sensor.Id}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["hue.type"] = Encoding.UTF8.GetBytes("sensor"),
                ["hue.id"] = Encoding.UTF8.GetBytes(sensor.Id),
                ["hue.sensor.type"] = Encoding.UTF8.GetBytes(sensor.Type ?? "unknown")
            }
        };
    }

    private SourceRecord CreateGroupRecord(Group group)
    {
        var payload = new
        {
            type = "group",
            id = group.Id,
            name = group.Name,
            groupType = group.Type?.ToString(),
            roomClass = group.Class?.ToString(),
            lights = group.Lights,
            state = new
            {
                allOn = group.State?.AllOn,
                anyOn = group.State?.AnyOn
            }
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "hue", ["type"] = "group" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["group_id"] = group.Id
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"group:{group.Id}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["hue.type"] = Encoding.UTF8.GetBytes("group"),
                ["hue.id"] = Encoding.UTF8.GetBytes(group.Id),
                ["hue.name"] = Encoding.UTF8.GetBytes(group.Name)
            }
        };
    }

    private SourceRecord CreateSceneRecord(Scene scene)
    {
        var payload = new
        {
            type = "scene",
            id = scene.Id,
            name = scene.Name,
            sceneType = scene.Type?.ToString(),
            group = scene.Group,
            lights = scene.Lights,
            owner = scene.Owner,
            recycle = scene.Recycle,
            locked = scene.Locked,
            lastUpdated = scene.LastUpdated
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "hue", ["type"] = "scene" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["scene_id"] = scene.Id
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"scene:{scene.Id}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["hue.type"] = Encoding.UTF8.GetBytes("scene"),
                ["hue.id"] = Encoding.UTF8.GetBytes(scene.Id),
                ["hue.name"] = Encoding.UTF8.GetBytes(scene.Name ?? "")
            }
        };
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
