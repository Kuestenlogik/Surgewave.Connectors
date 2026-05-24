using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.HomeGraphService.v1;
using Google.Apis.HomeGraphService.v1.Data;
using Google.Apis.Services;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Home;

/// <summary>
/// Task that controls Google Home smart home devices.
/// </summary>
public sealed class GoogleHomeSinkTask : SinkTask
{
    private HomeGraphServiceService? _service;
    private string _agentUserId = null!;
    private string? _defaultDeviceId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _agentUserId = config[GoogleHomeConnectorConfig.AgentUserId];
        _defaultDeviceId = config.TryGetValue(GoogleHomeConnectorConfig.DefaultDeviceId, out var defaultDeviceId) ? defaultDeviceId : null;

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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Get device ID from payload, headers, or default
                var deviceId = GetString(root, "deviceId", record.Headers) ?? _defaultDeviceId;
                if (string.IsNullOrEmpty(deviceId)) continue;

                // Build state from payload
                var state = BuildDeviceState(root);
                if (state.Count == 0) continue;

                // Report state change via Home Graph
                var request = new ReportStateAndNotificationRequest
                {
                    AgentUserId = _agentUserId,
                    RequestId = Guid.NewGuid().ToString(),
                    Payload = new StateAndNotificationPayload
                    {
                        Devices = new ReportStateAndNotificationDevice
                        {
                            States = new Dictionary<string, object>
                            {
                                [deviceId] = state
                            }
                        }
                    }
                };

                await _service!.Devices.ReportStateAndNotification(request).ExecuteAsync(cancellationToken);
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private static Dictionary<string, object> BuildDeviceState(JsonElement root)
    {
        var state = new Dictionary<string, object>();

        // On/Off
        if (root.TryGetProperty("on", out var onProp))
        {
            state["on"] = onProp.GetBoolean();
        }

        // Brightness
        if (root.TryGetProperty("brightness", out var briProp))
        {
            state["brightness"] = briProp.GetInt32();
        }

        // Color (HSV)
        if (root.TryGetProperty("color", out var colorProp))
        {
            var color = new Dictionary<string, object>();
            if (colorProp.TryGetProperty("spectrumHsv", out var hsv))
            {
                color["spectrumHsv"] = new Dictionary<string, object>
                {
                    ["hue"] = hsv.TryGetProperty("hue", out var h) ? h.GetDouble() : 0,
                    ["saturation"] = hsv.TryGetProperty("saturation", out var s) ? s.GetDouble() : 1,
                    ["value"] = hsv.TryGetProperty("value", out var v) ? v.GetDouble() : 1
                };
            }
            else if (colorProp.TryGetProperty("temperatureK", out var tempK))
            {
                color["temperatureK"] = tempK.GetInt32();
            }
            state["color"] = color;
        }

        // Color temperature only
        if (root.TryGetProperty("colorTemperature", out var ctProp))
        {
            state["color"] = new Dictionary<string, object>
            {
                ["temperatureK"] = ctProp.GetInt32()
            };
        }

        // Thermostat
        if (root.TryGetProperty("thermostatMode", out var modeProp))
        {
            state["thermostatMode"] = modeProp.GetString()!;
        }

        if (root.TryGetProperty("thermostatTemperatureSetpoint", out var setpointProp))
        {
            state["thermostatTemperatureSetpoint"] = setpointProp.GetDouble();
        }

        if (root.TryGetProperty("thermostatTemperatureAmbient", out var ambientProp))
        {
            state["thermostatTemperatureAmbient"] = ambientProp.GetDouble();
        }

        if (root.TryGetProperty("thermostatHumidityAmbient", out var humProp))
        {
            state["thermostatHumidityAmbient"] = humProp.GetDouble();
        }

        // Lock
        if (root.TryGetProperty("isLocked", out var lockedProp))
        {
            state["isLocked"] = lockedProp.GetBoolean();
        }

        if (root.TryGetProperty("isJammed", out var jammedProp))
        {
            state["isJammed"] = jammedProp.GetBoolean();
        }

        // Open/Close
        if (root.TryGetProperty("openPercent", out var openProp))
        {
            state["openPercent"] = openProp.GetInt32();
        }

        // Fan speed
        if (root.TryGetProperty("currentFanSpeedSetting", out var fanProp))
        {
            state["currentFanSpeedSetting"] = fanProp.GetString()!;
        }

        // Online status
        if (root.TryGetProperty("online", out var onlineProp))
        {
            state["online"] = onlineProp.GetBoolean();
        }
        else
        {
            state["online"] = true;  // Default to online
        }

        return state;
    }

    private static string? GetString(JsonElement element, string property, IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (element.TryGetProperty(property, out var prop))
            return prop.GetString();
        if (headers?.TryGetValue($"google.{property}", out var bytes) == true)
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
            _service?.Dispose();
        }
        base.Dispose(disposing);
    }
}
