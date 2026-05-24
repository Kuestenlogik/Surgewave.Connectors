using System.Text;
using Kuestenlogik.Surgewave.Connect;
using NNanomsg;
using NNanomsg.Protocols;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg;

/// <summary>
/// Task that receives messages from nanomsg sockets.
/// </summary>
public sealed class NanomsgSourceTask : SourceTask
{
    private NanomsgSocketBase? _socket;
    private string _topic = null!;
    private string _socketType = null!;
    private int _receiveTimeoutMs;
    private long _messageId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[NanomsgConnectorConfig.Topic];
        _socketType = (config.TryGetValue(NanomsgConnectorConfig.SocketType, out var socketType)
            ? socketType : NanomsgConnectorConfig.DefaultSocketType).ToUpperInvariant();

        var endpoints = config[NanomsgConnectorConfig.Endpoints]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var bindMode = (config.TryGetValue(NanomsgConnectorConfig.BindMode, out var bindModeVal) ? bindModeVal : "false") == "true";
        var rcvBufSize = int.Parse(config.TryGetValue(NanomsgConnectorConfig.ReceiveBufferSize, out var rcvBufSizeVal)
            ? rcvBufSizeVal : NanomsgConnectorConfig.DefaultReceiveBufferSize.ToString());
        var reconnectMs = int.Parse(config.TryGetValue(NanomsgConnectorConfig.ReconnectIntervalMs, out var reconnectMsVal)
            ? reconnectMsVal : NanomsgConnectorConfig.DefaultReconnectIntervalMs.ToString());
        _receiveTimeoutMs = int.Parse(config.TryGetValue(NanomsgConnectorConfig.ReceiveTimeoutMs, out var receiveTimeoutMs)
            ? receiveTimeoutMs : NanomsgConnectorConfig.DefaultReceiveTimeoutMs.ToString());

        // Create appropriate socket type
        _socket = _socketType switch
        {
            "SUB" => new SubscribeSocket(),
            "PULL" => new PullSocket(),
            "REP" => new ReplySocket(),
            "RESPONDENT" => new RespondentSocket(),
            "BUS" => new BusSocket(),
            "PAIR" => new PairSocket(),
            _ => throw new ArgumentException($"Unsupported socket type: {_socketType}")
        };

        NN.SetSockOpt(_socket.SocketID, SocketOption.RCVBUF, rcvBufSize);
        NN.SetSockOpt(_socket.SocketID, SocketOption.RECONNECT_IVL, reconnectMs);
        NN.SetSockOpt(_socket.SocketID, SocketOption.RCVTIMEO, _receiveTimeoutMs);

        // Connect or bind to endpoints
        foreach (var endpoint in endpoints)
        {
            if (bindMode && _socket is IBindSocket bindSocket)
                bindSocket.Bind(endpoint);
            else if (_socket is IConnectSocket connectSocket)
                connectSocket.Connect(endpoint);
        }

        // Subscribe for SUB socket
        if (_socket is SubscribeSocket subSocket)
        {
            var subscribeTopic = config.TryGetValue(NanomsgConnectorConfig.SubscribeTopic, out var subTopic) ? subTopic : "";
            subSocket.Subscribe(subscribeTopic);
        }
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            while (!cancellationToken.IsCancellationRequested && _socket is IReceiveSocket receiveSocket)
            {
                var data = receiveSocket.Receive();

                if (data != null && data.Length > 0)
                {
                    var record = CreateRecord(data);
                    records.Add(record);

                    // For REP/RESPONDENT sockets, send empty reply
                    if (_socket is ISendSocket sendSocket && (_socket is ReplySocket || _socket is RespondentSocket))
                    {
                        sendSocket.Send([]);
                    }
                }
                else
                {
                    // Timeout or no data
                    break;
                }

                // Limit batch size
                if (records.Count >= 100)
                    break;
            }
        }
        catch (NanomsgException)
        {
            // Timeout or resource unavailable - return what we have
        }
        catch (Exception)
        {
            // Log and continue
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    private SourceRecord CreateRecord(byte[] data)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "nanomsg", ["socket"] = _socketType },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = msgId },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(msgId.ToString()),
            Value = data,
            Headers = new Dictionary<string, byte[]>
            {
                ["nanomsg.socket.type"] = Encoding.UTF8.GetBytes(_socketType),
                ["nanomsg.size"] = Encoding.UTF8.GetBytes(data.Length.ToString())
            }
        };
    }

    public override void Stop()
    {
        _socket?.Dispose();
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
