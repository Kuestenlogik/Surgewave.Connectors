using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Twitter;

/// <summary>
/// Sink task that posts tweets to Twitter/X via API v2 using OAuth 1.0a.
/// </summary>
#pragma warning disable CA2213
public sealed class TwitterSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _consumerKey = string.Empty;
    private string _consumerSecret = string.Empty;
    private string _accessToken = string.Empty;
    private string _accessTokenSecret = string.Empty;
    private string _textField = "text";
    private string? _replyToField;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _consumerKey = config[TwitterConnectorConfig.ConsumerKey];
        _consumerSecret = config[TwitterConnectorConfig.ConsumerSecret];
        _accessToken = config[TwitterConnectorConfig.AccessToken];
        _accessTokenSecret = config[TwitterConnectorConfig.AccessTokenSecret];

        _textField = config.TryGetValue(TwitterConnectorConfig.TextField, out var tf) ? tf : "text";
        _replyToField = config.TryGetValue(TwitterConnectorConfig.ReplyToField, out var rtf) ? rtf : null;

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri("https://api.twitter.com/2/")
        };
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

                object payload;
                if (!string.IsNullOrEmpty(_replyToField) && data.TryGetValue(_replyToField, out var replyEl))
                {
                    var replyToId = replyEl.GetString();
                    payload = new { text, reply = new { in_reply_to_tweet_id = replyToId } };
                }
                else
                {
                    payload = new { text };
                }

                var payloadJson = JsonSerializer.Serialize(payload, JsonOptions);
                using var request = new HttpRequestMessage(HttpMethod.Post, new Uri("tweets", UriKind.Relative))
                {
                    Content = new StringContent(payloadJson, Encoding.UTF8, "application/json")
                };

                // Add OAuth 1.0a header
                var oauthHeader = GenerateOAuthHeader("POST", "https://api.twitter.com/2/tweets");
                request.Headers.Authorization = new AuthenticationHeaderValue("OAuth", oauthHeader);

                using var response = await _httpClient.SendAsync(request, cancellationToken);
            }
            catch (Exception)
            {
            }
        }
    }

#pragma warning disable CA5350 // HMACSHA1 required by Twitter OAuth 1.0a specification
    private string GenerateOAuthHeader(string method, string url)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var nonce = Guid.NewGuid().ToString("N");

        var parameters = new SortedDictionary<string, string>
        {
            ["oauth_consumer_key"] = _consumerKey,
            ["oauth_nonce"] = nonce,
            ["oauth_signature_method"] = "HMAC-SHA1",
            ["oauth_timestamp"] = timestamp,
            ["oauth_token"] = _accessToken,
            ["oauth_version"] = "1.0"
        };

        var paramString = string.Join("&", parameters.Select(kvp =>
            $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));

        var signatureBase = $"{method}&{Uri.EscapeDataString(url)}&{Uri.EscapeDataString(paramString)}";
        var signingKey = $"{Uri.EscapeDataString(_consumerSecret)}&{Uri.EscapeDataString(_accessTokenSecret)}";

        using var hmac = new HMACSHA1(Encoding.ASCII.GetBytes(signingKey));
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.ASCII.GetBytes(signatureBase)));

        parameters["oauth_signature"] = signature;

        return string.Join(", ", parameters.Select(kvp =>
            $"{Uri.EscapeDataString(kvp.Key)}=\"{Uri.EscapeDataString(kvp.Value)}\""));
    }
#pragma warning restore CA5350

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
