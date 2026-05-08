using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Q42.HueApi;
using Q42.HueApi.Interfaces;

namespace Kuestenlogik.Surgewave.Connector.Hue;

/// <summary>
/// Task that controls Philips Hue lights and groups.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Interface used for extensibility and testability")]
public sealed class HueSinkTask : SinkTask
{
    private ILocalHueClient? _client;
    private string? _defaultLightId;
    private string? _defaultGroupId;
    private int _transitionTimeMs;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var bridgeIp = config[HueConnectorConfig.BridgeIp];
        var appKey = config[HueConnectorConfig.AppKey];
        _defaultLightId = config.TryGetValue(HueConnectorConfig.DefaultLightId, out var defaultLightId) ? defaultLightId : null;
        _defaultGroupId = config.TryGetValue(HueConnectorConfig.DefaultGroupId, out var defaultGroupId) ? defaultGroupId : null;
        _transitionTimeMs = int.Parse(config.TryGetValue(HueConnectorConfig.TransitionTimeMs, out var transitionTimeMs)
            ? transitionTimeMs : HueConnectorConfig.DefaultTransitionTimeMs.ToString());

        _client = new LocalHueClient(bridgeIp);
        _client.Initialize(appKey);
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

                // Determine target type and ID
                var targetType = GetString(root, "type", record.Headers) ?? "light";
                var targetId = GetString(root, "id", record.Headers);

                if (string.IsNullOrEmpty(targetId))
                {
                    targetId = targetType == "group" ? _defaultGroupId : _defaultLightId;
                }

                if (string.IsNullOrEmpty(targetId)) continue;

                var command = new LightCommand();

                // Parse command properties
                if (root.TryGetProperty("on", out var onProp))
                    command.On = onProp.GetBoolean();
                if (root.TryGetProperty("brightness", out var briProp))
                    command.Brightness = (byte)briProp.GetInt32();
                if (root.TryGetProperty("hue", out var hueProp))
                    command.Hue = hueProp.GetInt32();
                if (root.TryGetProperty("saturation", out var satProp))
                    command.Saturation = satProp.GetInt32();
                if (root.TryGetProperty("colorTemperature", out var ctProp))
                    command.ColorTemperature = ctProp.GetInt32();
                if (root.TryGetProperty("transitionTime", out var ttProp))
                    command.TransitionTime = TimeSpan.FromMilliseconds(ttProp.GetInt32());
                else
                    command.TransitionTime = TimeSpan.FromMilliseconds(_transitionTimeMs);
                if (root.TryGetProperty("effect", out var effectProp))
                    command.Effect = effectProp.GetString() == "colorloop" ? Q42.HueApi.Effect.ColorLoop : Q42.HueApi.Effect.None;
                if (root.TryGetProperty("alert", out var alertProp))
                    command.Alert = alertProp.GetString() == "select" ? Q42.HueApi.Alert.Once : Q42.HueApi.Alert.None;

                // Check for scene activation
                if (root.TryGetProperty("scene", out var sceneProp))
                {
                    await _client!.RecallSceneAsync(sceneProp.GetString()!, targetId);
                }
                else if (targetType == "group")
                {
                    await _client!.SendGroupCommandAsync(command, targetId);
                }
                else
                {
                    await _client!.SendCommandAsync(command, [targetId]);
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private static string? GetString(JsonElement element, string property, IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (element.TryGetProperty(property, out var prop))
            return prop.GetString();
        if (headers?.TryGetValue($"hue.{property}", out var bytes) == true)
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
        base.Dispose(disposing);
    }
}
