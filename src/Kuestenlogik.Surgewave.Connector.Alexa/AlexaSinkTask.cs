using System.Collections.Frozen;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Alexa;

/// <summary>
/// Task that controls Alexa smart home devices.
/// </summary>
public sealed class AlexaSinkTask : SinkTask
{
    private HttpClient? _httpClient;
    private string _clientId = null!;
    private string _clientSecret = null!;
    private string _refreshToken = null!;
    private string _region = null!;
    private string? _defaultEndpointId;
    private string? _accessToken;
    private DateTime _tokenExpiry = DateTime.MinValue;

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
        _defaultEndpointId = config.TryGetValue(AlexaConnectorConfig.DefaultEndpointId, out var endpointId) ? endpointId : null;

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        await EnsureAccessTokenAsync(cancellationToken);
        if (string.IsNullOrEmpty(_accessToken)) return;

        var baseUrl = RegionEndpoints.TryGetValue(_region, out var url) ? url : RegionEndpoints["NA"];

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Get endpoint ID from payload, headers, or default
                var endpointId = GetString(root, "endpointId", record.Headers) ?? _defaultEndpointId;
                if (string.IsNullOrEmpty(endpointId)) continue;

                // Build directive based on command type
                var directive = BuildDirective(root, endpointId);
                if (directive == null) continue;

                // Send directive
                using var requestContent = new StringContent(directive, Encoding.UTF8, "application/json");
                using var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/v3/appliances/{endpointId}/directives");
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
                request.Content = requestContent;

                using var _ = await _httpClient!.SendAsync(request, cancellationToken);
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private string? BuildDirective(JsonElement root, string endpointId)
    {
        var @namespace = GetString(root, "namespace", null);
        var name = GetString(root, "name", null);

        // Infer directive from properties if not explicit
        if (string.IsNullOrEmpty(@namespace) || string.IsNullOrEmpty(name))
        {
            if (root.TryGetProperty("on", out var onProp))
            {
                @namespace = "Alexa.PowerController";
                name = onProp.GetBoolean() ? "TurnOn" : "TurnOff";
            }
            else if (root.TryGetProperty("brightness", out _))
            {
                @namespace = "Alexa.BrightnessController";
                name = "SetBrightness";
            }
            else if (root.TryGetProperty("color", out _))
            {
                @namespace = "Alexa.ColorController";
                name = "SetColor";
            }
            else if (root.TryGetProperty("colorTemperature", out _))
            {
                @namespace = "Alexa.ColorTemperatureController";
                name = "SetColorTemperature";
            }
            else if (root.TryGetProperty("targetTemperature", out _))
            {
                @namespace = "Alexa.ThermostatController";
                name = "SetTargetTemperature";
            }
            else if (root.TryGetProperty("lock", out var lockProp))
            {
                @namespace = "Alexa.LockController";
                name = lockProp.GetBoolean() ? "Lock" : "Unlock";
            }
            else
            {
                return null;
            }
        }

        var payload = BuildPayload(root, @namespace, name);

        var directive = new
        {
            directive = new
            {
                header = new
                {
                    @namespace,
                    name,
                    messageId = Guid.NewGuid().ToString(),
                    payloadVersion = "3"
                },
                endpoint = new
                {
                    endpointId,
                    scope = new
                    {
                        type = "BearerToken",
                        token = _accessToken
                    }
                },
                payload
            }
        };

        return JsonSerializer.Serialize(directive);
    }

    private static object BuildPayload(JsonElement root, string @namespace, string name)
    {
        return @namespace switch
        {
            "Alexa.BrightnessController" => new
            {
                brightness = root.TryGetProperty("brightness", out var br) ? br.GetInt32() : 100
            },
            "Alexa.ColorController" => new
            {
                color = root.TryGetProperty("color", out var color) ? new
                {
                    hue = color.TryGetProperty("hue", out var h) ? h.GetDouble() : 0,
                    saturation = color.TryGetProperty("saturation", out var s) ? s.GetDouble() : 1,
                    brightness = color.TryGetProperty("brightness", out var b) ? b.GetDouble() : 1
                } : new { hue = 0.0, saturation = 1.0, brightness = 1.0 }
            },
            "Alexa.ColorTemperatureController" => new
            {
                colorTemperatureInKelvin = root.TryGetProperty("colorTemperature", out var ct) ? ct.GetInt32() : 4000
            },
            "Alexa.ThermostatController" => new
            {
                targetSetpoint = new
                {
                    value = root.TryGetProperty("targetTemperature", out var temp) ? temp.GetDouble() : 21,
                    scale = root.TryGetProperty("scale", out var scale) ? scale.GetString() : "CELSIUS"
                }
            },
            "Alexa.PercentageController" => new
            {
                percentage = root.TryGetProperty("percentage", out var pct) ? pct.GetInt32() : 50
            },
            _ => new { }
        };
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

    private static string? GetString(JsonElement element, string property, IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (element.TryGetProperty(property, out var prop))
            return prop.GetString();
        if (headers?.TryGetValue($"alexa.{property}", out var bytes) == true)
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
