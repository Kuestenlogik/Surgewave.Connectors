using System.Text;
using System.Text.Json;
using MailKit;
using MailKit.Net.Imap;
using MailKit.Search;
using MimeKit;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Imap;

/// <summary>
/// Source task that reads emails from an IMAP server.
/// </summary>
public class ImapSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private IDictionary<string, string> _config = new Dictionary<string, string>();
    private string _topic = string.Empty;
    private ImapClient? _client;
    private IMailFolder? _folder;

    // Connection settings
    private string _host = string.Empty;
    private int _port = ImapConnectorConfig.DefaultPort;
    private string _username = string.Empty;
    private string _password = string.Empty;
    private bool _useSsl = ImapConnectorConfig.DefaultUseSsl;
    private int _timeoutSeconds = ImapConnectorConfig.DefaultTimeoutSeconds;
    private bool _acceptInvalidCertificates;

    // Folder settings
    private string _folderName = ImapConnectorConfig.DefaultFolder;

    // Polling settings
    private int _batchSize = ImapConnectorConfig.DefaultBatchSize;
    private bool _useIdle = ImapConnectorConfig.DefaultUseIdle;

    // Message handling
    private bool _markAsRead = ImapConnectorConfig.DefaultMarkAsRead;
    private bool _deleteAfterRead = ImapConnectorConfig.DefaultDeleteAfterRead;
    private bool _moveAfterRead;
    private string? _moveToFolder;
    private string _startFrom = ImapConnectorConfig.DefaultStartFrom;

    // Message filtering
    private bool _unseenOnly = ImapConnectorConfig.DefaultUnseenOnly;
    private DateTime? _sinceDate;
    private string? _subjectFilter;
    private string? _fromFilter;

    // Output settings
    private bool _includeBody = ImapConnectorConfig.DefaultIncludeBody;
    private bool _includeAttachments = ImapConnectorConfig.DefaultIncludeAttachments;
    private long _maxAttachmentSizeBytes = ImapConnectorConfig.DefaultMaxAttachmentSizeBytes;
    private bool _preferHtml = ImapConnectorConfig.DefaultPreferHtml;

    // Offset tracking
    private uint _lastUid;
    private bool _initialized;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Required settings
        _topic = config[ImapConnectorConfig.TopicConfig];
        _host = config[ImapConnectorConfig.HostConfig];
        _username = config[ImapConnectorConfig.UsernameConfig];

        if (config.TryGetValue(ImapConnectorConfig.PasswordConfig, out var password))
            _password = password;

        // Connection settings
        if (config.TryGetValue(ImapConnectorConfig.PortConfig, out var portStr) &&
            int.TryParse(portStr, out var port))
            _port = port;

        if (config.TryGetValue(ImapConnectorConfig.UseSslConfig, out var useSslStr) &&
            bool.TryParse(useSslStr, out var useSsl))
            _useSsl = useSsl;

        if (config.TryGetValue(ImapConnectorConfig.TimeoutSecondsConfig, out var timeoutStr) &&
            int.TryParse(timeoutStr, out var timeout))
            _timeoutSeconds = timeout;

        if (config.TryGetValue(ImapConnectorConfig.AcceptInvalidCertificatesConfig, out var acceptInvalidStr) &&
            bool.TryParse(acceptInvalidStr, out var acceptInvalid))
            _acceptInvalidCertificates = acceptInvalid;

        // Folder settings
        if (config.TryGetValue(ImapConnectorConfig.FolderConfig, out var folder) &&
            !string.IsNullOrWhiteSpace(folder))
            _folderName = folder;

        // Polling settings
        if (config.TryGetValue(ImapConnectorConfig.BatchSizeConfig, out var batchStr) &&
            int.TryParse(batchStr, out var batch))
            _batchSize = batch;

        if (config.TryGetValue(ImapConnectorConfig.UseIdleConfig, out var useIdleStr) &&
            bool.TryParse(useIdleStr, out var useIdle))
            _useIdle = useIdle;

        // Message handling
        if (config.TryGetValue(ImapConnectorConfig.MarkAsReadConfig, out var markReadStr) &&
            bool.TryParse(markReadStr, out var markRead))
            _markAsRead = markRead;

        if (config.TryGetValue(ImapConnectorConfig.DeleteAfterReadConfig, out var deleteStr) &&
            bool.TryParse(deleteStr, out var delete))
            _deleteAfterRead = delete;

        if (config.TryGetValue(ImapConnectorConfig.MoveAfterReadConfig, out var moveStr) &&
            bool.TryParse(moveStr, out var move))
            _moveAfterRead = move;

        if (config.TryGetValue(ImapConnectorConfig.MoveToFolderConfig, out var moveFolder))
            _moveToFolder = moveFolder;

        if (config.TryGetValue(ImapConnectorConfig.StartFromConfig, out var startFrom))
            _startFrom = startFrom;

        // Message filtering
        if (config.TryGetValue(ImapConnectorConfig.UnseenOnlyConfig, out var unseenStr) &&
            bool.TryParse(unseenStr, out var unseen))
            _unseenOnly = unseen;

        if (config.TryGetValue(ImapConnectorConfig.SinceConfig, out var since) &&
            DateTime.TryParse(since, out var sinceDate))
            _sinceDate = sinceDate;

        if (config.TryGetValue(ImapConnectorConfig.SubjectFilterConfig, out var subject))
            _subjectFilter = subject;

        if (config.TryGetValue(ImapConnectorConfig.FromFilterConfig, out var from))
            _fromFilter = from;

        // Output settings
        if (config.TryGetValue(ImapConnectorConfig.IncludeBodyConfig, out var bodyStr) &&
            bool.TryParse(bodyStr, out var body))
            _includeBody = body;

        if (config.TryGetValue(ImapConnectorConfig.IncludeAttachmentsConfig, out var attachStr) &&
            bool.TryParse(attachStr, out var attach))
            _includeAttachments = attach;

        if (config.TryGetValue(ImapConnectorConfig.MaxAttachmentSizeBytesConfig, out var maxSizeStr) &&
            long.TryParse(maxSizeStr, out var maxSize))
            _maxAttachmentSizeBytes = maxSize;

        if (config.TryGetValue(ImapConnectorConfig.PreferHtmlConfig, out var preferHtmlStr) &&
            bool.TryParse(preferHtmlStr, out var preferHtml))
            _preferHtml = preferHtml;
    }

    public override void Stop()
    {
        DisposeClient();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeClient();
        }
        base.Dispose(disposing);
    }

    private void DisposeClient()
    {
        if (_client != null)
        {
            if (_client.IsConnected)
            {
                try
                {
                    _client.Disconnect(true);
                }
                catch
                {
                    // Ignore disconnect errors
                }
            }
            _client.Dispose();
            _client = null;
        }
        _folder = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        await EnsureConnectedAsync(cancellationToken);

        if (_folder == null)
            return records;

        // Build search query
        var query = BuildSearchQuery();

        // Search for messages
        var uids = await _folder.SearchAsync(query, cancellationToken);

        if (uids.Count == 0)
            return records;

        // Filter by last processed UID if we're not starting from earliest
        var filteredUids = _initialized
            ? uids.Where(uid => uid.Id > _lastUid).OrderBy(uid => uid.Id).Take(_batchSize).ToList()
            : (_startFrom == ImapConnectorConfig.StartFromEarliest
                ? uids.OrderBy(uid => uid.Id).Take(_batchSize).ToList()
                : uids.OrderByDescending(uid => uid.Id).Take(1).ToList());

        if (filteredUids.Count == 0)
        {
            _initialized = true;
            return records;
        }

        // Fetch messages
        var messageSummaries = await _folder.FetchAsync(
            filteredUids,
            MessageSummaryItems.UniqueId | MessageSummaryItems.Envelope | MessageSummaryItems.Flags |
            MessageSummaryItems.Size | MessageSummaryItems.InternalDate,
            cancellationToken);

        var processedUids = new List<UniqueId>();

        foreach (var summary in messageSummaries.OrderBy(s => s.UniqueId.Id))
        {
            // Apply additional filters that couldn't be done server-side
            if (!PassesFilters(summary))
                continue;

            try
            {
                var message = await _folder.GetMessageAsync(summary.UniqueId, cancellationToken);
                var record = CreateSourceRecord(summary, message);
                records.Add(record);
                processedUids.Add(summary.UniqueId);

                if (summary.UniqueId.Id > _lastUid)
                    _lastUid = summary.UniqueId.Id;
            }
            catch (Exception ex)
            {
                // Log error and continue with next message
                Console.Error.WriteLine($"Error processing message UID {summary.UniqueId}: {ex.Message}");
            }
        }

        // Handle post-processing (mark as read, delete, move)
        if (processedUids.Count > 0)
        {
            await PostProcessMessagesAsync(processedUids, cancellationToken);
        }

        _initialized = true;
        return records;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Offset is tracked in memory - could persist to external store if needed
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the current offset for checkpoint/resume purposes.
    /// </summary>
    public IDictionary<string, object>? CurrentOffset =>
        _lastUid == 0
            ? null
            : new Dictionary<string, object>
            {
                [ImapConnectorConfig.OffsetUid] = _lastUid,
                [ImapConnectorConfig.OffsetFolder] = _folderName
            };

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_client != null && _client.IsConnected && _client.IsAuthenticated && _folder != null)
            return;

        // Cleanup existing client if needed
        if (_client != null)
        {
            try { _client.Disconnect(true); } catch { }
            _client.Dispose();
        }

        _client = new ImapClient();
        _client.Timeout = _timeoutSeconds * 1000;

        if (_acceptInvalidCertificates)
        {
#pragma warning disable CA5359 // Do not disable certificate validation - intentional opt-in feature
            _client.ServerCertificateValidationCallback = (s, c, h, e) => true;
#pragma warning restore CA5359
        }

        // Connect
        await _client.ConnectAsync(_host, _port, _useSsl, cancellationToken);

        // Authenticate
        await _client.AuthenticateAsync(_username, _password, cancellationToken);

        // Open folder
        _folder = await _client.GetFolderAsync(_folderName, cancellationToken);
        await _folder.OpenAsync(FolderAccess.ReadWrite, cancellationToken);
    }

    private SearchQuery BuildSearchQuery()
    {
        SearchQuery query = SearchQuery.All;

        if (_unseenOnly)
            query = query.And(SearchQuery.NotSeen);

        if (_sinceDate.HasValue)
            query = query.And(SearchQuery.DeliveredAfter(_sinceDate.Value));

        // Note: Subject and From filters are applied client-side for better matching
        return query;
    }

    private bool PassesFilters(IMessageSummary summary)
    {
        if (!string.IsNullOrEmpty(_subjectFilter))
        {
            var subject = summary.Envelope?.Subject ?? string.Empty;
            if (!subject.Contains(_subjectFilter, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        if (!string.IsNullOrEmpty(_fromFilter))
        {
            var from = summary.Envelope?.From?.ToString() ?? string.Empty;
            if (!from.Contains(_fromFilter, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private SourceRecord CreateSourceRecord(IMessageSummary summary, MimeMessage message)
    {
        var emailData = new Dictionary<string, object?>
        {
            ["uid"] = summary.UniqueId.Id,
            ["messageId"] = message.MessageId,
            ["subject"] = message.Subject,
            ["from"] = message.From.ToString(),
            ["to"] = message.To.ToString(),
            ["cc"] = message.Cc.Count > 0 ? message.Cc.ToString() : null,
            ["bcc"] = message.Bcc.Count > 0 ? message.Bcc.ToString() : null,
            ["replyTo"] = message.ReplyTo.Count > 0 ? message.ReplyTo.ToString() : null,
            ["date"] = message.Date.UtcDateTime.ToString("O"),
            ["internalDate"] = summary.InternalDate?.UtcDateTime.ToString("O"),
            ["folder"] = _folderName,
            ["size"] = summary.Size,
            ["flags"] = summary.Flags?.ToString()
        };

        // Add body if requested
        if (_includeBody)
        {
            if (_preferHtml && !string.IsNullOrEmpty(message.HtmlBody))
            {
                emailData["body"] = message.HtmlBody;
                emailData["bodyType"] = "html";
            }
            else if (!string.IsNullOrEmpty(message.TextBody))
            {
                emailData["body"] = message.TextBody;
                emailData["bodyType"] = "text";
            }
            else if (!string.IsNullOrEmpty(message.HtmlBody))
            {
                emailData["body"] = message.HtmlBody;
                emailData["bodyType"] = "html";
            }
        }

        // Add attachments if requested
        if (_includeAttachments)
        {
            var attachments = new List<Dictionary<string, object?>>();
            foreach (var attachment in message.Attachments)
            {
                if (attachment is MimePart mimePart)
                {
                    // MailKit 4.16 markiert MimePart.Content als nullable
                    // (vor 4.16 implicit non-null). Skip attachments ohne
                    // dekodierbaren Content statt zu NRE'en.
                    if (mimePart.Content is null) continue;

                    using var stream = new MemoryStream();
                    mimePart.Content.DecodeTo(stream);

                    if (stream.Length <= _maxAttachmentSizeBytes)
                    {
                        attachments.Add(new Dictionary<string, object?>
                        {
                            ["filename"] = mimePart.FileName ?? "attachment",
                            ["contentType"] = mimePart.ContentType.MimeType,
                            ["size"] = stream.Length,
                            ["content"] = Convert.ToBase64String(stream.ToArray())
                        });
                    }
                    else
                    {
                        attachments.Add(new Dictionary<string, object?>
                        {
                            ["filename"] = mimePart.FileName ?? "attachment",
                            ["contentType"] = mimePart.ContentType.MimeType,
                            ["size"] = stream.Length,
                            ["truncated"] = true
                        });
                    }
                }
            }

            if (attachments.Count > 0)
                emailData["attachments"] = attachments;
        }

        // Add headers
        var headers = new Dictionary<string, string>();
        foreach (var header in message.Headers)
        {
            if (!headers.ContainsKey(header.Field))
                headers[header.Field] = header.Value;
        }
        emailData["headers"] = headers;

        var json = JsonSerializer.Serialize(emailData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);

        // Use message ID as key if available, otherwise UID
        var key = !string.IsNullOrEmpty(message.MessageId)
            ? Encoding.UTF8.GetBytes(message.MessageId)
            : Encoding.UTF8.GetBytes($"{_folderName}:{summary.UniqueId.Id}");

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["host"] = _host,
                ["folder"] = _folderName
            },
            SourceOffset = new Dictionary<string, object>
            {
                [ImapConnectorConfig.OffsetUid] = summary.UniqueId.Id,
                [ImapConnectorConfig.OffsetFolder] = _folderName
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = message.Date.UtcDateTime,
            Headers = new Dictionary<string, byte[]>
            {
                ["imap.folder"] = Encoding.UTF8.GetBytes(_folderName),
                ["imap.uid"] = Encoding.UTF8.GetBytes(summary.UniqueId.Id.ToString()),
                ["imap.host"] = Encoding.UTF8.GetBytes(_host)
            }
        };
    }

    private async Task PostProcessMessagesAsync(List<UniqueId> uids, CancellationToken cancellationToken)
    {
        if (_folder == null)
            return;

        try
        {
            if (_markAsRead)
            {
                await _folder.AddFlagsAsync(uids, MessageFlags.Seen, true, cancellationToken);
            }

            if (_moveAfterRead && !string.IsNullOrEmpty(_moveToFolder))
            {
                var destFolder = await _client!.GetFolderAsync(_moveToFolder, cancellationToken);
                await _folder.MoveToAsync(uids, destFolder, cancellationToken);
            }
            else if (_deleteAfterRead)
            {
                await _folder.AddFlagsAsync(uids, MessageFlags.Deleted, true, cancellationToken);
                await _folder.ExpungeAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error post-processing messages: {ex.Message}");
        }
    }
}
