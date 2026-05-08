using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using NetMQ;
using NetMQ.Sockets;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ;

/// <summary>
/// Task that receives messages from ZeroMQ sockets.
/// </summary>
public sealed class ZeroMQSourceTask : SourceTask
{
    private NetMQSocket? _socket;
    private string _topic = null!;
    private string _socketType = null!;
    private string _messageFormat = null!;
    private TimeSpan _receiveTimeout;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[ZeroMQConnectorConfig.Topic];
        _socketType = (config.TryGetValue(ZeroMQConnectorConfig.SocketType, out var socketType)
            ? socketType : ZeroMQConnectorConfig.DefaultSocketType).ToUpperInvariant();
        _messageFormat = (config.TryGetValue(ZeroMQConnectorConfig.MessageFormat, out var messageFormat)
            ? messageFormat : ZeroMQConnectorConfig.DefaultMessageFormat).ToLowerInvariant();

        var endpoints = config[ZeroMQConnectorConfig.Endpoints]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var bindMode = (config.TryGetValue(ZeroMQConnectorConfig.BindMode, out var bindModeVal) ? bindModeVal : "false") == "true";
        var hwm = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.HighWaterMark, out var hwmVal)
            ? hwmVal : ZeroMQConnectorConfig.DefaultHighWaterMark.ToString());
        var lingerMs = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.LingerMs, out var lingerMsVal)
            ? lingerMsVal : ZeroMQConnectorConfig.DefaultLingerMs.ToString());
        var receiveTimeoutMs = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.ReceiveTimeoutMs, out var receiveTimeoutMsVal)
            ? receiveTimeoutMsVal : ZeroMQConnectorConfig.DefaultReceiveTimeoutMs.ToString());

        _receiveTimeout = TimeSpan.FromMilliseconds(receiveTimeoutMs);

        // Create appropriate socket type
        _socket = _socketType switch
        {
            "SUB" => new SubscriberSocket(),
            "PULL" => new PullSocket(),
            "REP" => new ResponseSocket(),
            _ => throw new ArgumentException($"Unsupported socket type: {_socketType}")
        };

        _socket.Options.ReceiveHighWatermark = hwm;
        _socket.Options.Linger = TimeSpan.FromMilliseconds(lingerMs);

        // Connect or bind to endpoints
        foreach (var endpoint in endpoints)
        {
            if (bindMode)
                _socket.Bind(endpoint);
            else
                _socket.Connect(endpoint);
        }

        // Subscribe to topics for SUB socket
        if (_socket is SubscriberSocket subSocket)
        {
            var subscribeTopics = config.TryGetValue(ZeroMQConnectorConfig.SubscribeTopics, out var subTopics) ? subTopics : "";
            if (string.IsNullOrWhiteSpace(subscribeTopics))
            {
                subSocket.SubscribeToAnyTopic();
            }
            else
            {
                var topics = subscribeTopics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                foreach (var t in topics)
                {
                    subSocket.Subscribe(t);
                }
            }
        }
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                NetMQMessage? message = null;

                if (_socket!.TryReceiveMultipartMessage(_receiveTimeout, ref message))
                {
                    if (message != null && message.FrameCount > 0)
                    {
                        var record = CreateRecord(message);
                        records.Add(record);

                        // For REP socket, send empty reply
                        if (_socket is ResponseSocket repSocket)
                        {
                            repSocket.SendFrameEmpty();
                        }
                    }
                }
                else
                {
                    // Timeout - return what we have
                    break;
                }

                // Limit batch size
                if (records.Count >= 100)
                    break;
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    private SourceRecord CreateRecord(NetMQMessage message)
    {
        var msgId = Interlocked.Increment(ref _messageId);
        byte[] key;
        byte[] value;
        var headers = new Dictionary<string, byte[]>
        {
            ["zeromq.socket.type"] = Encoding.UTF8.GetBytes(_socketType),
            ["zeromq.frame.count"] = Encoding.UTF8.GetBytes(message.FrameCount.ToString())
        };

        if (_messageFormat == "multipart")
        {
            // First frame is key, rest are value frames serialized as JSON array
            key = message.FrameCount > 1 ? message[0].Buffer : [];
            var frames = message.Skip(1).Select(f => Convert.ToBase64String(f.Buffer)).ToArray();
            value = JsonSerializer.SerializeToUtf8Bytes(new { frames });
        }
        else if (_socketType == "SUB" && message.FrameCount >= 2)
        {
            // SUB socket: first frame is topic prefix, second is data
            key = message[0].Buffer;
            value = message[1].Buffer;
            headers["zeromq.topic"] = message[0].Buffer;
        }
        else
        {
            // Single frame message
            key = Encoding.UTF8.GetBytes(msgId.ToString());
            value = message[0].Buffer;
        }

        // Format conversion
        if (_messageFormat == "json")
        {
            // Wrap raw bytes as JSON
            var payload = new
            {
                data = Convert.ToBase64String(value),
                encoding = "base64",
                timestamp = DateTime.UtcNow
            };
            value = JsonSerializer.SerializeToUtf8Bytes(payload);
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "zeromq", ["socket"] = _socketType },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = msgId },
            Topic = _topic,
            Key = key,
            Value = value,
            Headers = headers
        };
    }

    public override void Stop()
    {
        _socket?.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _socket?.Dispose();
        }
        base.Dispose(disposing);
    }
}
