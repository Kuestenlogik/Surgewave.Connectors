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
/// Task that sends XMPP messages.
/// </summary>
public sealed class XmppSinkTask : SinkTask
{
    private XmppClient? _client;
    private string? _defaultRecipient;
    private MessageType _defaultMessageType;
    private bool _isConnected;
    private IDisposable? _stateSubscription;

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

        _defaultRecipient = config.TryGetValue(XmppConnectorConfig.DefaultRecipient, out var defaultRecipient) ? defaultRecipient : null;
        var msgTypeStr = config.TryGetValue(XmppConnectorConfig.MessageType, out var msgType)
            ? msgType : XmppConnectorConfig.DefaultMessageType;
        _defaultMessageType = msgTypeStr.ToLowerInvariant() switch
        {
            "groupchat" => MessageType.GroupChat,
            "normal" => MessageType.Normal,
            "headline" => MessageType.Headline,
            _ => MessageType.Chat
        };

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

        // Subscribe to state changes using Rx
        _stateSubscription = _client.StateChanged
            .Subscribe(state => _isConnected = state == SessionState.Binded);

        // Connect asynchronously
        _ = _client.ConnectAsync();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Wait for connection if needed
        var retries = 0;
        while (!_isConnected && retries < 50)
        {
            await Task.Delay(100, cancellationToken);
            retries++;
        }

        if (!_isConnected) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                using var doc = JsonDocument.Parse(record.Value);
                var root = doc.RootElement;

                // Get recipient from payload, headers, or default
                var to = GetString(root, "to", record.Headers) ?? _defaultRecipient;
                if (string.IsNullOrEmpty(to)) continue;

                // Get message body
                var body = GetString(root, "body", record.Headers);
                if (string.IsNullOrEmpty(body)) continue;

                // Get message type
                var msgType = _defaultMessageType;
                var typeStr = GetString(root, "type", record.Headers);
                if (!string.IsNullOrEmpty(typeStr))
                {
                    msgType = typeStr.ToLowerInvariant() switch
                    {
                        "groupchat" => MessageType.GroupChat,
                        "normal" => MessageType.Normal,
                        "headline" => MessageType.Headline,
                        _ => MessageType.Chat
                    };
                }

                // Create and send message
                var message = new Message
                {
                    To = to,
                    Type = msgType,
                    Body = body
                };

                // Optional subject
                var subject = GetString(root, "subject", record.Headers);
                if (!string.IsNullOrEmpty(subject))
                {
                    message.Subject = subject;
                }

                // Optional thread
                var thread = GetString(root, "thread", record.Headers);
                if (!string.IsNullOrEmpty(thread))
                {
                    message.Thread = thread;
                }

                await _client!.SendAsync(message);
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
        if (headers?.TryGetValue($"xmpp.{property}", out var bytes) == true)
            return Encoding.UTF8.GetString(bytes);
        return null;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _stateSubscription?.Dispose();
        if (_client != null)
        {
            _ = _client.DisconnectAsync();
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _stateSubscription?.Dispose();
            // XmppClient cleanup happens in Stop() via DisconnectAsync
        }
        base.Dispose(disposing);
    }
}
