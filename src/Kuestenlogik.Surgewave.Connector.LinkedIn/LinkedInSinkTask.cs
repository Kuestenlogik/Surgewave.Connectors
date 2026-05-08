using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.LinkedIn;

/// <summary>
/// Sink task that posts content to LinkedIn via Marketing API.
/// </summary>
#pragma warning disable CA2213
public sealed class LinkedInSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _accessToken = string.Empty;
    private string? _organizationId;
    private string? _personId;
    private string _textField = "text";
    private string _defaultVisibility = "PUBLIC";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[LinkedInConnectorConfig.AccessToken];
        _organizationId = config.TryGetValue(LinkedInConnectorConfig.OrganizationId, out var oid) ? oid : null;
        _personId = config.TryGetValue(LinkedInConnectorConfig.PersonId, out var pid) ? pid : null;

        _textField = config.TryGetValue(LinkedInConnectorConfig.TextField, out var tf) ? tf : "text";
        _defaultVisibility = config.TryGetValue(LinkedInConnectorConfig.DefaultVisibility, out var dv)
            ? dv : LinkedInConnectorConfig.DefaultVisibilityValue;

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(LinkedInConnectorConfig.BaseUrl)
        };
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
        _httpClient.DefaultRequestHeaders.Add("X-Restli-Protocol-Version", "2.0.0");
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
                if (data == null) continue;

                var text = data.TryGetValue(_textField, out var textEl) ? textEl.GetString() : json;
                if (string.IsNullOrEmpty(text)) continue;

                // Determine author URN
                string authorUrn;
                if (!string.IsNullOrEmpty(_organizationId))
                {
                    authorUrn = $"urn:li:organization:{_organizationId}";
                }
                else if (!string.IsNullOrEmpty(_personId))
                {
                    authorUrn = $"urn:li:person:{_personId}";
                }
                else
                {
                    continue; // No author specified
                }

                var payload = new
                {
                    author = authorUrn,
                    lifecycleState = "PUBLISHED",
                    specificContent = new
                    {
                        comLinkedinUgcShareContent = new
                        {
                            shareCommentary = new { text },
                            shareMediaCategory = "NONE"
                        }
                    },
                    visibility = new
                    {
                        comLinkedinUgcMemberNetworkVisibility = _defaultVisibility
                    }
                };

                using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");
                using var response = await _httpClient.PostAsync(new Uri("/v2/ugcPosts", UriKind.Relative), content, cancellationToken);
            }
            catch (Exception)
            {
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
        _httpClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Stop();
        base.Dispose(disposing);
    }
}
#pragma warning restore CA2213
