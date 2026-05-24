using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using XmppDotNet;
using XmppDotNet.Transport.Socket;
using XmppDotNet.Xmpp;
using XmppDotNet.Xmpp.Client;

namespace Kuestenlogik.Surgewave.Connector.Xmpp;

/// <summary>
/// Task that receives XMPP messages and presence updates.
/// </summary>
public sealed class XmppSourceTask : SourceTask
{
    private XmppClient? _client;
    private string _topic = null!;
    private bool _includePresence;
    private bool _includeGroupChat;
    private HashSet<string>? _filterJids;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _messageId;
    private IDisposable? _messageSubscription;
    private IDisposable? _presenceSubscription;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var host = config[XmppConnectorConfig.Host];
        var port = int.Parse(config.TryGetValue(XmppConnectorConfig.Port, out var portVal)
            ? portVal : XmppConnectorConfig.DefaultPort.ToString());
        var domain = config[XmppConnectorConfig.Domain];
        var username = config[XmppConnectorConfig.Username];
        var password = config[XmppConnectorConfig.Password];
        var resource = config.TryGetValue(XmppConnectorConfig.Resource, out var res)
            ? res : XmppConnectorConfig.DefaultResource;
        var useTls = (config.TryGetValue(XmppConnectorConfig.UseTls, out var useTlsVal) ? useTlsVal : "true") == "true";

        _topic = config[XmppConnectorConfig.Topic];
        _includePresence = (config.TryGetValue(XmppConnectorConfig.IncludePresence, out var includePresence) ? includePresence : "true") == "true";
        _includeGroupChat = (config.TryGetValue(XmppConnectorConfig.IncludeGroupChat, out var includeGroupChat) ? includeGroupChat : "true") == "true";

        var filterJidsStr = config.TryGetValue(XmppConnectorConfig.FilterJids, out var filterJids) ? filterJids : "";
        if (!string.IsNullOrWhiteSpace(filterJidsStr))
        {
            _filterJids = filterJidsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        // Use host as domain if not using SRV records
        // For custom host/port, configure DNS or use domain that resolves to the host
        _client = new XmppClient(conf =>
        {
            conf.UseSocketTransport();
            conf.AutoReconnect = true;
        })
        {
            Jid = $"{username}@{host}/{resource}",  // Use host as domain for direct connection
            Password = password,
            Tls = useTls
        };

        // Subscribe to messages using Rx
        _messageSubscription = _client.XmppXElementReceived
            .OfType<Message>()
            .Subscribe(OnMessageReceived);

        if (_includePresence)
        {
            _presenceSubscription = _client.XmppXElementReceived
                .OfType<Presence>()
                .Subscribe(OnPresenceReceived);
        }

        // Connect asynchronously
        _ = ConnectAsync(config);
    }

    private async Task ConnectAsync(IDictionary<string, string> config)
    {
        try
        {
            await _client!.ConnectAsync();

            // Join MUC rooms if configured
            var joinRoomsStr = config.TryGetValue(XmppConnectorConfig.JoinRooms, out var joinRooms) ? joinRooms : "";
            if (!string.IsNullOrWhiteSpace(joinRoomsStr))
            {
                var rooms = joinRoomsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                foreach (var room in rooms)
                {
                    try
                    {
                        // Send presence to MUC room to join
                        var mucPresence = new Presence
                        {
                            To = $"{room}/{_client.Jid.Local}"
                        };
                        await _client.SendAsync(mucPresence);
                    }
                    catch (Exception)
                    {
                        // Log and continue
                    }
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private void OnMessageReceived(Message message)
    {
        try
        {
            if (message.Type == MessageType.GroupChat && !_includeGroupChat)
                return;

            var fromJid = message.From?.Bare;
            if (_filterJids != null && fromJid != null && !_filterJids.Contains(fromJid))
                return;

            var record = CreateMessageRecord(message);
            _pendingRecords.Enqueue(record);
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private void OnPresenceReceived(Presence presence)
    {
        try
        {
            var fromJid = presence.From?.Bare;
            if (_filterJids != null && fromJid != null && !_filterJids.Contains(fromJid))
                return;

            var record = CreatePresenceRecord(presence);
            _pendingRecords.Enqueue(record);
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    private SourceRecord CreateMessageRecord(Message message)
    {
        var payload = new
        {
            type = "message",
            messageType = message.Type.ToString().ToLowerInvariant(),
            from = message.From?.ToString(),
            fromBare = message.From?.Bare,
            to = message.To?.ToString(),
            body = message.Body,
            subject = message.Subject,
            thread = message.Thread,
            id = message.Id,
            timestamp = DateTime.UtcNow
        };

        var msgId = Interlocked.Increment(ref _messageId);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "xmpp", ["type"] = "message" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["xmpp_id"] = message.Id ?? msgId.ToString()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(message.From?.Bare ?? "unknown"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["xmpp.type"] = Encoding.UTF8.GetBytes("message"),
                ["xmpp.message.type"] = Encoding.UTF8.GetBytes(message.Type.ToString().ToLowerInvariant()),
                ["xmpp.from"] = Encoding.UTF8.GetBytes(message.From?.Bare ?? "unknown")
            }
        };
    }

    private SourceRecord CreatePresenceRecord(Presence presence)
    {
        var payload = new
        {
            type = "presence",
            presenceType = presence.Type.ToString().ToLowerInvariant(),
            from = presence.From?.ToString(),
            fromBare = presence.From?.Bare,
            show = presence.Show.ToString().ToLowerInvariant(),
            status = presence.Status,
            priority = presence.Priority,
            timestamp = DateTime.UtcNow
        };

        var msgId = Interlocked.Increment(ref _messageId);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "xmpp", ["type"] = "presence" },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["jid"] = presence.From?.Bare ?? "unknown"
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(presence.From?.Bare ?? "unknown"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["xmpp.type"] = Encoding.UTF8.GetBytes("presence"),
                ["xmpp.show"] = Encoding.UTF8.GetBytes(presence.Show.ToString().ToLowerInvariant()),
                ["xmpp.from"] = Encoding.UTF8.GetBytes(presence.From?.Bare ?? "unknown")
            }
        };
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override void Stop()
    {
        _messageSubscription?.Dispose();
        _presenceSubscription?.Dispose();
        if (_client != null)
        {
            _ = _client.DisconnectAsync();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _messageSubscription?.Dispose();
            _presenceSubscription?.Dispose();
            // XmppClient cleanup happens in Stop() via DisconnectAsync
        }
        base.Dispose(disposing);
    }
}
