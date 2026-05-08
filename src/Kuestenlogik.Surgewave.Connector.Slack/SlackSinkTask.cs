using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using SlackNet;
using SlackNet.Blocks;
using SlackNet.WebApi;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Slack;

/// <summary>
/// Sink task that posts messages to Slack via the Web API.
/// </summary>
public partial class SlackSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    // Slack client
    private ISlackApiClient? _apiClient;

    // Settings
    private string? _defaultChannel;
    private string? _channelField;
    private string? _textField;
    private string? _textTemplate;
    private string? _blocksField;
    private string? _attachmentsField;
    private string? _threadTsField;
    private string? _username;
    private string? _iconEmoji;
    private string? _iconUrl;
    private bool _unfurlLinks = SlackConnectorConfig.DefaultUnfurlLinks;
    private bool _unfurlMedia = SlackConnectorConfig.DefaultUnfurlMedia;
    private bool _addReaction;
    private string? _reactionField;
    private string? _defaultReaction;
    private int _retryCount = SlackConnectorConfig.DefaultRetryCount;
    private int _retryDelayMs = SlackConnectorConfig.DefaultRetryDelayMs;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        var botToken = config[SlackConnectorConfig.BotTokenConfig];

        // Channel settings
        config.TryGetValue(SlackConnectorConfig.DefaultChannelConfig, out _defaultChannel);
        config.TryGetValue(SlackConnectorConfig.ChannelFieldConfig, out _channelField);

        // Message content
        config.TryGetValue(SlackConnectorConfig.TextFieldConfig, out _textField);
        _textTemplate = config.TryGetValue(SlackConnectorConfig.TextTemplateConfig, out var template)
            ? template
            : SlackConnectorConfig.DefaultTextTemplate;
        config.TryGetValue(SlackConnectorConfig.BlocksFieldConfig, out _blocksField);
        config.TryGetValue(SlackConnectorConfig.AttachmentsFieldConfig, out _attachmentsField);

        // Threading
        config.TryGetValue(SlackConnectorConfig.ThreadTsFieldConfig, out _threadTsField);

        // Appearance
        config.TryGetValue(SlackConnectorConfig.UsernameConfig, out _username);
        config.TryGetValue(SlackConnectorConfig.IconEmojiConfig, out _iconEmoji);
        config.TryGetValue(SlackConnectorConfig.IconUrlConfig, out _iconUrl);

        // Formatting
        if (config.TryGetValue(SlackConnectorConfig.UnfurlLinksConfig, out var unfurlLinks) &&
            bool.TryParse(unfurlLinks, out var unfurlLinksValue))
        {
            _unfurlLinks = unfurlLinksValue;
        }

        if (config.TryGetValue(SlackConnectorConfig.UnfurlMediaConfig, out var unfurlMedia) &&
            bool.TryParse(unfurlMedia, out var unfurlMediaValue))
        {
            _unfurlMedia = unfurlMediaValue;
        }

        // Reactions
        if (config.TryGetValue(SlackConnectorConfig.AddReactionConfig, out var addReaction) &&
            bool.TryParse(addReaction, out var addReactionValue))
        {
            _addReaction = addReactionValue;
        }

        config.TryGetValue(SlackConnectorConfig.ReactionFieldConfig, out _reactionField);
        config.TryGetValue(SlackConnectorConfig.DefaultReactionConfig, out _defaultReaction);

        // Behavior
        if (config.TryGetValue(SlackConnectorConfig.RetryCountConfig, out var retryCount) &&
            int.TryParse(retryCount, out var retryCountValue))
        {
            _retryCount = retryCountValue;
        }

        if (config.TryGetValue(SlackConnectorConfig.RetryDelayMsConfig, out var retryDelay) &&
            int.TryParse(retryDelay, out var retryDelayValue))
        {
            _retryDelayMs = retryDelayValue;
        }

        // Initialize Slack client
        _apiClient = new SlackServiceBuilder()
            .UseApiToken(botToken)
            .GetApiClient();
    }

    public override void Stop()
    {
        _apiClient = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_apiClient == null)
            throw new InvalidOperationException("Slack client not initialized");

        foreach (var record in records)
        {
            await ProcessRecordWithRetryAsync(record, cancellationToken);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private async Task ProcessRecordWithRetryAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var lastException = default(Exception);

        for (var attempt = 0; attempt <= _retryCount; attempt++)
        {
            try
            {
                if (_addReaction)
                {
                    await AddReactionAsync(record, cancellationToken);
                }
                else
                {
                    await PostMessageAsync(record, cancellationToken);
                }
                return;
            }
            catch (SlackException ex) when (ex.ErrorCode == "rate_limited" && attempt < _retryCount)
            {
                // Handle rate limiting with exponential backoff
                await Task.Delay(_retryDelayMs * (attempt + 1), cancellationToken);
                lastException = ex;
            }
            catch (Exception ex) when (attempt < _retryCount)
            {
                await Task.Delay(_retryDelayMs * (attempt + 1), cancellationToken);
                lastException = ex;
            }
        }

        throw lastException ?? new InvalidOperationException("Unknown error during Slack API call");
    }

    private async Task PostMessageAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_apiClient == null)
            return;

        var (channel, json) = ParseRecord(record);
        if (string.IsNullOrEmpty(channel))
            throw new InvalidOperationException("No channel specified for message");

        var message = new Message
        {
            Channel = channel,
            Text = GetMessageText(record, json),
            UnfurlLinks = _unfurlLinks,
            UnfurlMedia = _unfurlMedia
        };

        // Set username if configured
        if (!string.IsNullOrEmpty(_username))
        {
            message.Username = _username;
        }

        // Set icon
        if (!string.IsNullOrEmpty(_iconEmoji))
        {
            message.IconEmoji = _iconEmoji;
        }
        else if (!string.IsNullOrEmpty(_iconUrl))
        {
            message.IconUrl = _iconUrl;
        }

        // Set thread timestamp for replies
        if (!string.IsNullOrEmpty(_threadTsField) && json.HasValue)
        {
            if (json.Value.TryGetProperty(_threadTsField, out var threadTs))
            {
                message.ThreadTs = threadTs.GetString();
            }
        }

        // Set blocks if configured
        if (!string.IsNullOrEmpty(_blocksField) && json.HasValue)
        {
            if (json.Value.TryGetProperty(_blocksField, out var blocks) && blocks.ValueKind == JsonValueKind.Array)
            {
                message.Blocks = ParseBlocks(blocks);
            }
        }

        // Set attachments if configured
        if (!string.IsNullOrEmpty(_attachmentsField) && json.HasValue)
        {
            if (json.Value.TryGetProperty(_attachmentsField, out var attachments) && attachments.ValueKind == JsonValueKind.Array)
            {
                message.Attachments = ParseAttachments(attachments);
            }
        }

        await _apiClient.Chat.PostMessage(message, cancellationToken);
    }

    private async Task AddReactionAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_apiClient == null)
            return;

        var (channel, json) = ParseRecord(record);
        if (string.IsNullOrEmpty(channel))
            throw new InvalidOperationException("No channel specified for reaction");

        // Get reaction emoji
        string? reaction = null;
        if (!string.IsNullOrEmpty(_reactionField) && json.HasValue)
        {
            if (json.Value.TryGetProperty(_reactionField, out var reactionElement))
            {
                reaction = reactionElement.GetString();
            }
        }

        reaction ??= _defaultReaction;

        if (string.IsNullOrEmpty(reaction))
            throw new InvalidOperationException("No reaction specified");

        // Get timestamp of message to react to
        string? timestamp = null;
        if (json.HasValue)
        {
            if (json.Value.TryGetProperty("ts", out var ts))
            {
                timestamp = ts.GetString();
            }
            else if (json.Value.TryGetProperty("timestamp", out var timestamp2))
            {
                timestamp = timestamp2.GetString();
            }
        }

        if (string.IsNullOrEmpty(timestamp))
            throw new InvalidOperationException("No timestamp specified for reaction target");

        var reactionName = reaction.TrimStart(':').TrimEnd(':');
        await _apiClient.Reactions.AddToMessage(reactionName, channel, timestamp, cancellationToken);
    }

    private (string? channel, JsonElement? json) ParseRecord(SinkRecord record)
    {
        JsonElement? json = null;
        string? channel = _defaultChannel;

        // Try to parse value as JSON
        if (record.Value != null && record.Value.Length > 0)
        {
            try
            {
                var valueStr = Encoding.UTF8.GetString(record.Value);
                using var doc = JsonDocument.Parse(valueStr);
                json = doc.RootElement.Clone();

                // Extract channel from JSON if configured
                if (!string.IsNullOrEmpty(_channelField) && json.Value.TryGetProperty(_channelField, out var channelElement))
                {
                    channel = channelElement.GetString() ?? channel;
                }
            }
            catch (JsonException)
            {
                // Not JSON, use as plain text
            }
        }

        return (channel, json);
    }

    private string GetMessageText(SinkRecord record, JsonElement? json)
    {
        // If text field is specified, try to get from JSON
        if (!string.IsNullOrEmpty(_textField) && json.HasValue)
        {
            if (json.Value.TryGetProperty(_textField, out var textElement))
            {
                return textElement.GetString() ?? string.Empty;
            }
        }

        // Use template
        if (string.IsNullOrEmpty(_textTemplate))
            return string.Empty;

        var text = _textTemplate;

        // Replace ${value}
        if (record.Value != null)
        {
            var valueStr = Encoding.UTF8.GetString(record.Value);
            text = text.Replace("${value}", valueStr);
        }

        // Replace ${key}
        if (record.Key != null)
        {
            var keyStr = Encoding.UTF8.GetString(record.Key);
            text = text.Replace("${key}", keyStr);
        }

        // Replace ${field.name} patterns
        if (json.HasValue)
        {
            text = FieldRegex().Replace(text, match =>
            {
                var fieldPath = match.Groups[1].Value;
                return GetJsonFieldValue(json.Value, fieldPath);
            });
        }

        return text;
    }

    private static string GetJsonFieldValue(JsonElement json, string fieldPath)
    {
        var current = json;
        var parts = fieldPath.Split('.');

        foreach (var part in parts)
        {
            if (current.ValueKind != JsonValueKind.Object)
                return string.Empty;

            if (!current.TryGetProperty(part, out current))
                return string.Empty;
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString() ?? string.Empty,
            JsonValueKind.Number => current.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => string.Empty,
            _ => current.GetRawText()
        };
    }

    private static List<Block> ParseBlocks(JsonElement blocksElement)
    {
        var blocks = new List<Block>();

        foreach (var blockElement in blocksElement.EnumerateArray())
        {
            var blockJson = blockElement.GetRawText();
            var block = JsonSerializer.Deserialize<Block>(blockJson, JsonOptions);
            if (block != null)
            {
                blocks.Add(block);
            }
        }

        return blocks;
    }

    private static List<Attachment> ParseAttachments(JsonElement attachmentsElement)
    {
        var attachments = new List<Attachment>();

        foreach (var attachmentElement in attachmentsElement.EnumerateArray())
        {
            var attachmentJson = attachmentElement.GetRawText();
            var attachment = JsonSerializer.Deserialize<Attachment>(attachmentJson, JsonOptions);
            if (attachment != null)
            {
                attachments.Add(attachment);
            }
        }

        return attachments;
    }

    [GeneratedRegex(@"\$\{([^}]+)\}")]
    private static partial Regex FieldRegex();
}
