using System.Text;
using Kuestenlogik.Surgewave.Connect;
using NetMQ;
using NetMQ.Sockets;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ;

/// <summary>
/// Task that sends messages to ZeroMQ sockets.
/// </summary>
public sealed class ZeroMQSinkTask : SinkTask
{
    private NetMQSocket? _socket;
    private string _socketType = null!;
    private string? _publishTopic;
    private TimeSpan _sendTimeout;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _socketType = (config.TryGetValue(ZeroMQConnectorConfig.SocketType, out var socketType) ? socketType : "PUB").ToUpperInvariant();
        _publishTopic = config.TryGetValue(ZeroMQConnectorConfig.PublishTopic, out var publishTopic) ? publishTopic : null;

        var endpoints = config[ZeroMQConnectorConfig.Endpoints]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var bindMode = (config.TryGetValue(ZeroMQConnectorConfig.BindMode, out var bindModeVal) ? bindModeVal : "true") == "true";
        var hwm = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.HighWaterMark, out var hwmVal)
            ? hwmVal : ZeroMQConnectorConfig.DefaultHighWaterMark.ToString());
        var lingerMs = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.LingerMs, out var lingerMsVal)
            ? lingerMsVal : ZeroMQConnectorConfig.DefaultLingerMs.ToString());
        var sendTimeoutMs = int.Parse(config.TryGetValue(ZeroMQConnectorConfig.SendTimeoutMs, out var sendTimeoutMsVal)
            ? sendTimeoutMsVal : ZeroMQConnectorConfig.DefaultSendTimeoutMs.ToString());

        _sendTimeout = TimeSpan.FromMilliseconds(sendTimeoutMs);

        // Create appropriate socket type
        _socket = _socketType switch
        {
            "PUB" => new PublisherSocket(),
            "PUSH" => new PushSocket(),
            "REQ" => new RequestSocket(),
            _ => throw new ArgumentException($"Unsupported socket type: {_socketType}")
        };

        _socket.Options.SendHighWatermark = hwm;
        _socket.Options.Linger = TimeSpan.FromMilliseconds(lingerMs);

        // Connect or bind to endpoints
        foreach (var endpoint in endpoints)
        {
            if (bindMode)
                _socket.Bind(endpoint);
            else
                _socket.Connect(endpoint);
        }

        // Give PUB socket time to establish connections
        if (_socket is PublisherSocket)
        {
            Thread.Sleep(100);
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                // Determine topic prefix for PUB socket
                string? topic = null;
                if (_socket is PublisherSocket)
                {
                    topic = _publishTopic;

                    // Check for topic in headers
                    if (record.Headers?.TryGetValue("zeromq.topic", out var topicBytes) == true)
                    {
                        topic = Encoding.UTF8.GetString(topicBytes);
                    }
                }

                if (_socket is PublisherSocket && !string.IsNullOrEmpty(topic))
                {
                    // PUB socket with topic: send as multipart [topic, data]
                    var msg = new NetMQMessage();
                    msg.Append(topic);
                    msg.Append(record.Value);
                    _socket.TrySendMultipartMessage(_sendTimeout, msg);
                }
                else if (_socket is RequestSocket reqSocket)
                {
                    // REQ socket: send and wait for reply
                    if (reqSocket.TrySendFrame(_sendTimeout, record.Value))
                    {
                        // Wait for reply (required for REQ/REP pattern)
                        reqSocket.TryReceiveFrameBytes(_sendTimeout, out _);
                    }
                }
                else
                {
                    // PUSH or PUB without topic: send single frame
                    _socket!.TrySendFrame(_sendTimeout, record.Value);
                }
            }
            catch (Exception)
            {
                // Log and continue
            }
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
