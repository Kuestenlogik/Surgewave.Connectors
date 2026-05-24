using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Matter;

/// <summary>
/// Task that controls Matter devices via a Matter controller.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Interface used for extensibility")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "URL strings are simpler for REST API calls")]
public sealed class MatterSinkTask : SinkTask
{
    private HttpClient? _httpClient;
    private string _controllerUrl = null!;
    private string? _defaultNodeId;
    private int _defaultEndpointId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _controllerUrl = config[MatterConnectorConfig.ControllerUrl].TrimEnd('/');
        _defaultNodeId = config.TryGetValue(MatterConnectorConfig.DefaultNodeId, out var defaultNodeId) ? defaultNodeId : null;
        _defaultEndpointId = int.Parse(config.TryGetValue(MatterConnectorConfig.DefaultEndpointId, out var defaultEndpointId)
            ? defaultEndpointId : MatterConnectorConfig.DefaultEndpointIdValue.ToString());

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json");

        var apiKey = config.TryGetValue(MatterConnectorConfig.ApiKey, out var apiKeyVal) ? apiKeyVal : null;
        if (!string.IsNullOrWhiteSpace(apiKey))
        {
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Get node ID from payload, headers, or default
                var nodeId = GetString(root, "nodeId", record.Headers) ?? _defaultNodeId;
                if (string.IsNullOrEmpty(nodeId)) continue;

                // Get endpoint ID
                var endpointId = _defaultEndpointId;
                if (root.TryGetProperty("endpointId", out var epProp))
                    endpointId = epProp.GetInt32();

                // Determine command type
                var command = GetString(root, "command", record.Headers);

                if (command != null)
                {
                    await ExecuteCommandAsync(nodeId, endpointId, command, root, cancellationToken);
                }
                else
                {
                    // Infer command from state properties
                    await ExecuteStateCommandsAsync(nodeId, endpointId, root, cancellationToken);
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private async Task ExecuteCommandAsync(string nodeId, int endpointId, string command, JsonElement root, CancellationToken ct)
    {
        var cluster = GetString(root, "cluster", null) ?? InferClusterFromCommand(command);
        var args = root.TryGetProperty("args", out var argsProp) ? argsProp : default;

        var payload = new
        {
            node_id = nodeId,
            endpoint_id = endpointId,
            cluster,
            command,
            args
        };

        var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
    }

    private async Task ExecuteStateCommandsAsync(string nodeId, int endpointId, JsonElement root, CancellationToken ct)
    {
        // Handle on/off
        if (root.TryGetProperty("on", out var onProp))
        {
            var command = onProp.GetBoolean() ? "On" : "Off";
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "OnOff",
                command
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }

        // Handle brightness (level control)
        if (root.TryGetProperty("brightness", out var briProp))
        {
            var level = briProp.GetInt32();
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "LevelControl",
                command = "MoveToLevel",
                args = new { level, transition_time = 10 }
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }

        // Handle color temperature
        if (root.TryGetProperty("colorTemperature", out var ctProp))
        {
            var colorTemp = ctProp.GetInt32();
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "ColorControl",
                command = "MoveToColorTemperature",
                args = new { color_temperature_mireds = colorTemp, transition_time = 10 }
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }

        // Handle hue/saturation
        if (root.TryGetProperty("hue", out var hueProp) && root.TryGetProperty("saturation", out var satProp))
        {
            var hue = hueProp.GetInt32();
            var saturation = satProp.GetInt32();
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "ColorControl",
                command = "MoveToHueAndSaturation",
                args = new { hue, saturation, transition_time = 10 }
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }

        // Handle thermostat setpoint
        if (root.TryGetProperty("heatingSetpoint", out var heatProp))
        {
            var setpoint = (int)(heatProp.GetDouble() * 100);
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "Thermostat",
                command = "SetpointRaiseLower",
                args = new { mode = 0, amount = setpoint }
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }

        // Handle door lock
        if (root.TryGetProperty("lock", out var lockProp))
        {
            var command = lockProp.GetBoolean() ? "LockDoor" : "UnlockDoor";
            var payload = new
            {
                node_id = nodeId,
                endpoint_id = endpointId,
                cluster = "DoorLock",
                command
            };
            var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            await _httpClient!.PostAsync($"{_controllerUrl}/api/command", content, ct);
        }
    }

    private static string InferClusterFromCommand(string command)
    {
        return command.ToLowerInvariant() switch
        {
            "on" or "off" or "toggle" => "OnOff",
            "movetolevel" or "move" or "step" => "LevelControl",
            "movetohue" or "movetosaturation" or "movetohuesaturation" or "movetocolortemperature" => "ColorControl",
            "lockdoor" or "unlockdoor" => "DoorLock",
            "setpointraiselow" => "Thermostat",
            _ => "OnOff"
        };
    }

    private static string? GetString(JsonElement element, string property, IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (element.TryGetProperty(property, out var prop))
            return prop.GetString();
        if (headers?.TryGetValue($"matter.{property}", out var bytes) == true)
            return Encoding.UTF8.GetString(bytes);
        return null;
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
