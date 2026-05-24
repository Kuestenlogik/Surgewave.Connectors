using System.Text;
using System.Text.Json;
using Azure.Identity;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Teams;

/// <summary>
/// Task that polls Microsoft Teams channels for new messages via Graph API.
/// </summary>
public sealed class TeamsSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private GraphServiceClient? _graphClient;
    private string _teamId = "";
    private string _channelId = "";
    private int _pollIntervalMs = TeamsConnectorConfig.DefaultPollIntervalMs;
    private bool _includeReplies = TeamsConnectorConfig.DefaultIncludeReplies;
    private DateTimeOffset _lastPollTime;
    private string? _lastMessageId;
    private long _offset;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[TeamsConnectorConfig.Topic];

        var tenantId = config[TeamsConnectorConfig.TenantId];
        var clientId = config[TeamsConnectorConfig.ClientId];
        var clientSecret = config[TeamsConnectorConfig.ClientSecret];

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        _graphClient = new GraphServiceClient(credential);

        _teamId = config[TeamsConnectorConfig.TeamId];
        _channelId = config[TeamsConnectorConfig.ChannelId];

        if (config.TryGetValue(TeamsConnectorConfig.PollIntervalMs, out var pollMs))
            _pollIntervalMs = int.Parse(pollMs);

        if (config.TryGetValue(TeamsConnectorConfig.IncludeReplies, out var replies))
            _includeReplies = bool.Parse(replies);

        _lastPollTime = DateTimeOffset.UtcNow;
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _graphClient?.Dispose();
            _graphClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (_graphClient == null) return records;

        try
        {
            // Get messages from the channel
            var messages = await _graphClient.Teams[_teamId].Channels[_channelId].Messages
                .GetAsync(config =>
                {
                    config.QueryParameters.Top = 50;
                    config.QueryParameters.Orderby = ["createdDateTime desc"];
                }, cancellationToken);

            if (messages?.Value == null) return records;

            // Process new messages (newer than last poll time)
            foreach (var message in messages.Value.OrderBy(m => m.CreatedDateTime))
            {
                // Skip messages we've already processed
                if (message.Id == _lastMessageId) continue;
                if (message.CreatedDateTime <= _lastPollTime) continue;

                var record = CreateRecord(message);
                if (record != null)
                    records.Add(record);

                // Process replies if enabled
                if (_includeReplies && message.Id != null)
                {
                    try
                    {
                        var replies = await _graphClient.Teams[_teamId].Channels[_channelId]
                            .Messages[message.Id].Replies
                            .GetAsync(cancellationToken: cancellationToken);

                        if (replies?.Value != null)
                        {
                            foreach (var reply in replies.Value.OrderBy(r => r.CreatedDateTime))
                            {
                                if (reply.CreatedDateTime <= _lastPollTime) continue;

                                var replyRecord = CreateRecord(reply, message.Id);
                                if (replyRecord != null)
                                    records.Add(replyRecord);
                            }
                        }
                    }
                    catch
                    {
                        // Ignore reply fetch errors
                    }
                }
            }

            if (messages.Value.Count > 0)
            {
                var latest = messages.Value.MaxBy(m => m.CreatedDateTime);
                if (latest != null)
                {
                    _lastMessageId = latest.Id;
                    if (latest.CreatedDateTime.HasValue)
                        _lastPollTime = latest.CreatedDateTime.Value;
                }
            }
        }
        catch (Exception ex)
        {
            Context.RaiseError?.Invoke(ex);
        }

        // Wait before next poll if no messages
        if (records.Count == 0)
        {
            try
            {
                await Task.Delay(Math.Min(_pollIntervalMs, 1000), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Normal cancellation
            }
        }

        return records;
    }

    private SourceRecord? CreateRecord(ChatMessage message, string? parentId = null)
    {
        if (string.IsNullOrEmpty(message.Body?.Content)) return null;

        var offset = Interlocked.Increment(ref _offset);
        var headers = new Dictionary<string, byte[]>
        {
            ["teams_message_id"] = Encoding.UTF8.GetBytes(message.Id ?? ""),
            ["teams_from_user"] = Encoding.UTF8.GetBytes(message.From?.User?.DisplayName ?? ""),
            ["teams_from_id"] = Encoding.UTF8.GetBytes(message.From?.User?.Id ?? ""),
            ["teams_content_type"] = Encoding.UTF8.GetBytes(message.Body?.ContentType?.ToString() ?? "text")
        };

        if (!string.IsNullOrEmpty(message.Subject))
            headers["teams_subject"] = Encoding.UTF8.GetBytes(message.Subject);

        if (parentId != null)
            headers["teams_parent_id"] = Encoding.UTF8.GetBytes(parentId);

        if (message.Importance != null)
            headers["teams_importance"] = Encoding.UTF8.GetBytes(message.Importance.Value.ToString().ToLowerInvariant());

        // Serialize message to JSON
        var messageData = new
        {
            id = message.Id,
            subject = message.Subject,
            body = message.Body?.Content,
            contentType = message.Body?.ContentType?.ToString(),
            from = new
            {
                userId = message.From?.User?.Id,
                displayName = message.From?.User?.DisplayName
            },
            createdDateTime = message.CreatedDateTime,
            importance = message.Importance?.ToString(),
            parentMessageId = parentId,
            attachments = message.Attachments?.Select(a => new
            {
                id = a.Id,
                name = a.Name,
                contentType = a.ContentType,
                contentUrl = a.ContentUrl
            }).ToList()
        };

        var value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(messageData));

        return new SourceRecord
        {
            Topic = _topic,
            Partition = 0,
            SourcePartition = new Dictionary<string, object>
            {
                ["teamId"] = _teamId,
                ["channelId"] = _channelId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["offset"] = offset,
                ["messageId"] = message.Id ?? ""
            },
            Key = message.Id != null ? Encoding.UTF8.GetBytes(message.Id) : null,
            Value = value,
            Headers = headers,
            Timestamp = message.CreatedDateTime
        };
    }
}
