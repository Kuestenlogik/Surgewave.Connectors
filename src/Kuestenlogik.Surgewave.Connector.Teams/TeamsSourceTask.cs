using System.Globalization;
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
    private const int PageSize = 50;
    private const int MaxPagesPerPoll = 20;

    public override string Version => "1.0.0";

    private string _topic = "";
    private GraphServiceClient? _graphClient;
    private string _teamId = "";
    private string _channelId = "";
    private int _pollIntervalMs = TeamsConnectorConfig.DefaultPollIntervalMs;
    private bool _includeReplies = TeamsConnectorConfig.DefaultIncludeReplies;
    private readonly Dictionary<string, object> _sourcePartition = [];

    /// <summary>Resume point: messages created at or before this are done.</summary>
    private DateTimeOffset _cursor;

    /// <summary>Resume point that becomes valid once the batch in flight is fully committed.</summary>
    private DateTimeOffset _batchCursor;

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

        _sourcePartition[TeamsConnectorConfig.PartitionTeamId] = _teamId;
        _sourcePartition[TeamsConnectorConfig.PartitionChannelId] = _channelId;

        // Resume where the previous run committed; only a fresh connector starts at "now".
        _cursor = ReadStoredCursor() ?? DateTimeOffset.UtcNow;
        _batchCursor = _cursor;
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
            var messages = await FetchNewMessagesAsync(cancellationToken);

            // Resume point that is still valid while the current message group is in flight.
            var groupCursor = _cursor;

            foreach (var message in messages)
            {
                List<SourceRecord>? replyRecords = null;

                if (_includeReplies && message.Id != null)
                {
                    try
                    {
                        replyRecords = await FetchRepliesAsync(message.Id, groupCursor, cancellationToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        // Surface it and end the batch here: the cursor must not move past a message
                        // whose replies could not be read, so the whole group is retried next poll.
                        Context?.RaiseError?.Invoke(ex);
                        break;
                    }
                }

                var record = CreateRecord(message, groupCursor);
                if (record != null)
                    records.Add(record);

                if (replyRecords != null)
                    records.AddRange(replyRecords);

                if (message.CreatedDateTime.HasValue)
                    groupCursor = message.CreatedDateTime.Value;
            }

            _batchCursor = groupCursor;

            // Nothing to commit, so the scanned range can be skipped straight away.
            if (records.Count == 0)
                _cursor = _batchCursor;
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(ex);
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

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        if (record.SourceOffset.TryGetValue(TeamsConnectorConfig.OffsetCursor, out var raw) &&
            TryParseCursor(raw, out var cursor) &&
            cursor > _cursor)
        {
            _cursor = cursor;
        }
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Every record of the batch is committed, so its newest group is complete.
        if (_batchCursor > _cursor)
            _cursor = _batchCursor;

        return Task.CompletedTask;
    }

    private DateTimeOffset? ReadStoredCursor()
    {
        var stored = Context?.OffsetStorageReader?.Offset(_sourcePartition);

        if (stored != null &&
            stored.TryGetValue(TeamsConnectorConfig.OffsetCursor, out var raw) &&
            TryParseCursor(raw, out var cursor))
        {
            return cursor;
        }

        return null;
    }

    private static bool TryParseCursor(object? raw, out DateTimeOffset cursor)
    {
        if (raw is DateTimeOffset stored)
        {
            cursor = stored;
            return true;
        }

        return DateTimeOffset.TryParse(
            raw?.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out cursor);
    }

    /// <summary>
    /// Reads every channel message created after the cursor, following the Graph paging links so
    /// that bursts larger than a single page are not skipped.
    /// </summary>
    private async Task<List<ChatMessage>> FetchNewMessagesAsync(CancellationToken cancellationToken)
    {
        var newMessages = new List<ChatMessage>();

        var page = await _graphClient!.Teams[_teamId].Channels[_channelId].Messages
            .GetAsync(request =>
            {
                request.QueryParameters.Top = PageSize;
                request.QueryParameters.Orderby = ["createdDateTime desc"];
            }, cancellationToken);

        for (var pages = 0; pages < MaxPagesPerPoll && page is not null; pages++)
        {
            var reachedCursor = false;

            foreach (var message in page.Value ?? [])
            {
                // Newest first, so the first already-processed message ends the scan.
                if ((message.CreatedDateTime ?? DateTimeOffset.MinValue) <= _cursor)
                {
                    reachedCursor = true;
                    break;
                }

                newMessages.Add(message);
            }

            var nextLink = page.OdataNextLink;
            if (reachedCursor || string.IsNullOrEmpty(nextLink)) break;

            page = await _graphClient.Teams[_teamId].Channels[_channelId].Messages
                .WithUrl(nextLink)
                .GetAsync(cancellationToken: cancellationToken);
        }

        // Emit oldest first so the cursor only ever moves forward.
        return [.. newMessages.OrderBy(m => m.CreatedDateTime)];
    }

    private async Task<List<SourceRecord>> FetchRepliesAsync(
        string messageId, DateTimeOffset groupCursor, CancellationToken cancellationToken)
    {
        var replies = new List<ChatMessage>();

        var page = await _graphClient!.Teams[_teamId].Channels[_channelId]
            .Messages[messageId].Replies
            .GetAsync(request => request.QueryParameters.Top = PageSize, cancellationToken);

        for (var pages = 0; pages < MaxPagesPerPoll && page is not null; pages++)
        {
            replies.AddRange(page.Value ?? []);

            var nextLink = page.OdataNextLink;
            if (string.IsNullOrEmpty(nextLink)) break;

            page = await _graphClient.Teams[_teamId].Channels[_channelId]
                .Messages[messageId].Replies
                .WithUrl(nextLink)
                .GetAsync(cancellationToken: cancellationToken);
        }

        var records = new List<SourceRecord>();

        foreach (var reply in replies.OrderBy(r => r.CreatedDateTime))
        {
            if ((reply.CreatedDateTime ?? DateTimeOffset.MinValue) <= groupCursor) continue;

            var replyRecord = CreateRecord(reply, groupCursor, messageId);
            if (replyRecord != null)
                records.Add(replyRecord);
        }

        return records;
    }

    private SourceRecord? CreateRecord(ChatMessage message, DateTimeOffset groupCursor, string? parentId = null)
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

        // The cursor carried by a record is the resume point that is safe if this record is the
        // last one committed - the group it belongs to is replayed instead of being skipped.
        var sourceOffset = new Dictionary<string, object>
        {
            ["offset"] = offset,
            [TeamsConnectorConfig.OffsetCursor] = groupCursor.ToString("O", CultureInfo.InvariantCulture),
            [TeamsConnectorConfig.OffsetMessageId] = message.Id ?? ""
        };

        if (parentId != null)
            sourceOffset[TeamsConnectorConfig.OffsetParentMessageId] = parentId;

        return new SourceRecord
        {
            Topic = _topic,
            Partition = 0,
            SourcePartition = new Dictionary<string, object>(_sourcePartition),
            SourceOffset = sourceOffset,
            Key = message.Id != null ? Encoding.UTF8.GetBytes(message.Id) : null,
            Value = value,
            Headers = headers,
            Timestamp = message.CreatedDateTime
        };
    }
}
