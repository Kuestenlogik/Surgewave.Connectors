using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using SlackNet;
using SlackNet.Events;
using SlackNet.SocketMode;
using SystemChannel = System.Threading.Channels.Channel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Slack;

/// <summary>
/// Source task that receives events from Slack via Socket Mode.
/// </summary>
public class SlackSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private IDictionary<string, string> _config = new Dictionary<string, string>();
    private string _topic = string.Empty;

    // Slack clients
    private ISlackApiClient? _apiClient;
    private ISlackSocketModeClient? _socketModeClient;

    // Settings
    private HashSet<string> _eventTypes = new();
    private HashSet<string>? _channelFilter;
    private HashSet<string>? _userFilter;
    private bool _includeBotMessages = SlackConnectorConfig.DefaultIncludeBotMessages;

    // Event buffer
    private readonly Channel<Event> _eventChannel = SystemChannel.CreateBounded<Event>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

    // Offset tracking
    private string? _lastTimestamp;
    private string? _lastChannel;
    private bool _connected;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
        _topic = config[SlackConnectorConfig.TopicConfig];

        var botToken = config[SlackConnectorConfig.BotTokenConfig];

        // Parse event types
        var eventTypesStr = SlackConnectorConfig.DefaultEventTypes;
        if (config.TryGetValue(SlackConnectorConfig.EventTypesConfig, out var types) &&
            !string.IsNullOrWhiteSpace(types))
        {
            eventTypesStr = types;
        }
        _eventTypes = eventTypesStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Parse channel filter
        if (config.TryGetValue(SlackConnectorConfig.ChannelFilterConfig, out var channels) &&
            !string.IsNullOrWhiteSpace(channels))
        {
            _channelFilter = channels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        // Parse user filter
        if (config.TryGetValue(SlackConnectorConfig.UserFilterConfig, out var users) &&
            !string.IsNullOrWhiteSpace(users))
        {
            _userFilter = users.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        // Include bot messages
        if (config.TryGetValue(SlackConnectorConfig.IncludeBotMessagesConfig, out var includeBots) &&
            bool.TryParse(includeBots, out var includeBotsValue))
        {
            _includeBotMessages = includeBotsValue;
        }

        // Initialize Slack client
        _apiClient = new SlackServiceBuilder()
            .UseApiToken(botToken)
            .GetApiClient();
    }

    public override void Stop()
    {
        _socketModeClient?.Dispose();
        _socketModeClient = null;
        _connected = false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _socketModeClient?.Dispose();
            _socketModeClient = null;
            _connected = false;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Ensure connected
        await EnsureConnectedAsync(cancellationToken);

        // Read available events from channel (non-blocking)
        var maxEvents = 100;
        while (records.Count < maxEvents && _eventChannel.Reader.TryRead(out var eventData))
        {
            var record = CreateSourceRecord(eventData);
            if (record != null)
            {
                records.Add(record);
            }
        }

        // If no events available, wait briefly for one
        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(1));

                if (await _eventChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    while (records.Count < maxEvents && _eventChannel.Reader.TryRead(out var eventData))
                    {
                        var record = CreateSourceRecord(eventData);
                        if (record != null)
                        {
                            records.Add(record);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Timeout waiting for events, return empty
            }
        }

        return records;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the current offset for checkpoint/resume purposes.
    /// </summary>
    public IDictionary<string, object>? CurrentOffset =>
        _lastTimestamp == null
            ? null
            : new Dictionary<string, object>
            {
                [SlackConnectorConfig.OffsetTimestamp] = _lastTimestamp,
                [SlackConnectorConfig.OffsetChannel] = _lastChannel ?? string.Empty
            };

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_connected && _socketModeClient != null)
            return;

        // Get app token for socket mode
        if (!_config.TryGetValue(SlackConnectorConfig.AppTokenConfig, out var appToken) ||
            string.IsNullOrWhiteSpace(appToken))
        {
            throw new InvalidOperationException("App token required for Socket Mode");
        }

        var botToken = _config[SlackConnectorConfig.BotTokenConfig];

        // Create socket mode client
        _socketModeClient = new SlackServiceBuilder()
            .UseApiToken(botToken)
            .UseAppLevelToken(appToken)
            .RegisterEventHandler<MessageEvent>(new EventHandler<MessageEvent>(_eventChannel.Writer, PassesFilter))
            .RegisterEventHandler<ReactionAdded>(new EventHandler<ReactionAdded>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeReactionAdded)))
            .RegisterEventHandler<ReactionRemoved>(new EventHandler<ReactionRemoved>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeReactionRemoved)))
            .RegisterEventHandler<AppMention>(new EventHandler<AppMention>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeAppMention)))
            .RegisterEventHandler<ChannelCreated>(new EventHandler<ChannelCreated>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeChannelCreated)))
            .RegisterEventHandler<ChannelArchive>(new EventHandler<ChannelArchive>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeChannelArchive)))
            .RegisterEventHandler<MemberJoinedChannel>(new EventHandler<MemberJoinedChannel>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeMemberJoined)))
            .RegisterEventHandler<MemberLeftChannel>(new EventHandler<MemberLeftChannel>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeMemberLeft)))
            .RegisterEventHandler<FileShared>(new EventHandler<FileShared>(_eventChannel.Writer, _ => _eventTypes.Contains(SlackConnectorConfig.EventTypeFileShared)))
            .GetSocketModeClient();

        await _socketModeClient.Connect();
        _connected = true;
    }

    private bool PassesFilter(MessageEvent message)
    {
        // Check event type
        if (!_eventTypes.Contains(SlackConnectorConfig.EventTypeMessage))
            return false;

        // Check bot messages
        if (!_includeBotMessages && !string.IsNullOrEmpty(message.BotId))
            return false;

        // Check channel filter
        if (_channelFilter != null && !_channelFilter.Contains(message.Channel))
            return false;

        // Check user filter
        if (_userFilter != null && !string.IsNullOrEmpty(message.User) && !_userFilter.Contains(message.User))
            return false;

        return true;
    }

    private SourceRecord? CreateSourceRecord(Event slackEvent)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["type"] = slackEvent.Type,
            ["event_ts"] = GetEventTimestamp(slackEvent),
            ["event_time"] = slackEvent.EventTs
        };

        string? channel = null;
        string? timestamp = null;
        string? user = null;

        switch (slackEvent)
        {
            case MessageEvent msg:
                eventData["channel"] = msg.Channel;
                eventData["user"] = msg.User;
                eventData["text"] = msg.Text;
                eventData["ts"] = msg.Ts;
                eventData["thread_ts"] = msg.ThreadTs;
                eventData["bot_id"] = msg.BotId;
                eventData["subtype"] = msg.Subtype;
                channel = msg.Channel;
                timestamp = msg.Ts;
                user = msg.User;
                break;

            case ReactionAdded reaction:
                eventData["user"] = reaction.User;
                eventData["reaction"] = reaction.Reaction;
                eventData["item_type"] = reaction.Item?.Type;
                timestamp = reaction.EventTs;
                user = reaction.User;
                break;

            case ReactionRemoved reaction:
                eventData["user"] = reaction.User;
                eventData["reaction"] = reaction.Reaction;
                eventData["item_type"] = reaction.Item?.Type;
                timestamp = reaction.EventTs;
                user = reaction.User;
                break;

            case AppMention mention:
                eventData["channel"] = mention.Channel;
                eventData["user"] = mention.User;
                eventData["text"] = mention.Text;
                eventData["ts"] = mention.Ts;
                eventData["thread_ts"] = mention.ThreadTs;
                channel = mention.Channel;
                timestamp = mention.Ts;
                user = mention.User;
                break;

            case ChannelCreated created:
                eventData["channel_id"] = created.Channel?.Id;
                eventData["channel_name"] = created.Channel?.Name;
                eventData["creator"] = created.Channel?.Creator;
                channel = created.Channel?.Id;
                timestamp = created.EventTs;
                break;

            case ChannelArchive archive:
                eventData["channel"] = archive.Channel;
                eventData["user"] = archive.User;
                channel = archive.Channel;
                timestamp = archive.EventTs;
                user = archive.User;
                break;

            case MemberJoinedChannel joined:
                eventData["channel"] = joined.Channel;
                eventData["user"] = joined.User;
                eventData["inviter"] = joined.Inviter;
                channel = joined.Channel;
                timestamp = joined.EventTs;
                user = joined.User;
                break;

            case MemberLeftChannel left:
                eventData["channel"] = left.Channel;
                eventData["user"] = left.User;
                channel = left.Channel;
                timestamp = left.EventTs;
                user = left.User;
                break;

            case FileShared fileShared:
                eventData["channel_id"] = fileShared.ChannelId;
                eventData["file_id"] = fileShared.FileId;
                eventData["user_id"] = fileShared.UserId;
                channel = fileShared.ChannelId;
                timestamp = fileShared.EventTs;
                break;
        }

        // Update offset tracking
        if (timestamp != null)
        {
            _lastTimestamp = timestamp;
            _lastChannel = channel;
        }

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);

        // Use timestamp as key
        var key = timestamp != null
            ? Encoding.UTF8.GetBytes(timestamp)
            : Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["channel"] = channel ?? "unknown"
            },
            SourceOffset = new Dictionary<string, object>
            {
                [SlackConnectorConfig.OffsetTimestamp] = timestamp ?? string.Empty,
                [SlackConnectorConfig.OffsetChannel] = channel ?? string.Empty
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = ParseSlackTimestamp(timestamp),
            Headers = new Dictionary<string, byte[]>
            {
                ["slack.event.type"] = Encoding.UTF8.GetBytes(slackEvent.Type),
                ["slack.channel"] = Encoding.UTF8.GetBytes(channel ?? string.Empty),
                ["slack.user"] = Encoding.UTF8.GetBytes(user ?? string.Empty)
            }
        };
    }

    private static string? GetEventTimestamp(Event slackEvent)
    {
        return slackEvent switch
        {
            MessageEvent msg => msg.Ts,
            ReactionAdded reaction => reaction.EventTs,
            ReactionRemoved reaction => reaction.EventTs,
            AppMention mention => mention.Ts,
            _ => slackEvent.EventTs
        };
    }

    private static DateTimeOffset ParseSlackTimestamp(string? ts)
    {
        if (string.IsNullOrEmpty(ts))
            return DateTimeOffset.UtcNow;

        // Slack timestamps are Unix epoch seconds with microseconds after decimal
        var parts = ts.Split('.');
        if (parts.Length >= 1 && long.TryParse(parts[0], out var seconds))
        {
            return DateTimeOffset.FromUnixTimeSeconds(seconds);
        }

        return DateTimeOffset.UtcNow;
    }

    private sealed class EventHandler<T> : IEventHandler<T> where T : Event
    {
        private readonly ChannelWriter<Event> _writer;
        private readonly Func<T, bool> _filter;

        public EventHandler(ChannelWriter<Event> writer, Func<T, bool> filter)
        {
            _writer = writer;
            _filter = filter;
        }

        public Task Handle(T slackEvent)
        {
            if (_filter(slackEvent))
            {
                _writer.TryWrite(slackEvent);
            }
            return Task.CompletedTask;
        }
    }
}
