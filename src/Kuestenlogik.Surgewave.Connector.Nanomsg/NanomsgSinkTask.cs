using Kuestenlogik.Surgewave.Connect;
using NNanomsg;
using NNanomsg.Protocols;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg;

/// <summary>
/// Task that sends messages to nanomsg sockets.
/// </summary>
public sealed class NanomsgSinkTask : SinkTask
{
    private NanomsgSocketBase? _socket;
    private string _socketType = null!;
    private int _sendTimeoutMs;
    private int _surveyDeadlineMs;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _socketType = (config.TryGetValue(NanomsgConnectorConfig.SocketType, out var socketType) ? socketType : "PUB").ToUpperInvariant();

        var endpoints = config[NanomsgConnectorConfig.Endpoints]
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var bindMode = (config.TryGetValue(NanomsgConnectorConfig.BindMode, out var bindModeVal) ? bindModeVal : "true") == "true";
        var sndBufSize = int.Parse(config.TryGetValue(NanomsgConnectorConfig.SendBufferSize, out var sndBufSizeVal)
            ? sndBufSizeVal : NanomsgConnectorConfig.DefaultSendBufferSize.ToString());
        var reconnectMs = int.Parse(config.TryGetValue(NanomsgConnectorConfig.ReconnectIntervalMs, out var reconnectMsVal)
            ? reconnectMsVal : NanomsgConnectorConfig.DefaultReconnectIntervalMs.ToString());
        _sendTimeoutMs = int.Parse(config.TryGetValue(NanomsgConnectorConfig.SendTimeoutMs, out var sendTimeoutMs)
            ? sendTimeoutMs : NanomsgConnectorConfig.DefaultSendTimeoutMs.ToString());
        _surveyDeadlineMs = int.Parse(config.TryGetValue(NanomsgConnectorConfig.SurveyDeadlineMs, out var surveyDeadlineMs)
            ? surveyDeadlineMs : NanomsgConnectorConfig.DefaultSurveyDeadlineMs.ToString());

        // Create appropriate socket type
        _socket = _socketType switch
        {
            "PUB" => new PublishSocket(),
            "PUSH" => new PushSocket(),
            "REQ" => new RequestSocket(),
            "SURVEYOR" => new SurveyorSocket(),
            "BUS" => new BusSocket(),
            "PAIR" => new PairSocket(),
            _ => throw new ArgumentException($"Unsupported socket type: {_socketType}")
        };

        NN.SetSockOpt(_socket.SocketID, SocketOption.SNDBUF, sndBufSize);
        NN.SetSockOpt(_socket.SocketID, SocketOption.RECONNECT_IVL, reconnectMs);
        NN.SetSockOpt(_socket.SocketID, SocketOption.SNDTIMEO, _sendTimeoutMs);

        if (_socket is SurveyorSocket)
        {
            NN.SetSockOpt(_socket.SocketID, SocketOption.SURVEYOR_DEADLINE, _surveyDeadlineMs);
        }

        // Connect or bind to endpoints
        foreach (var endpoint in endpoints)
        {
            if (bindMode && _socket is IBindSocket bindSocket)
                bindSocket.Bind(endpoint);
            else if (_socket is IConnectSocket connectSocket)
                connectSocket.Connect(endpoint);
        }

        // Give PUB socket time to establish connections
        if (_socket is PublishSocket)
        {
            Thread.Sleep(100);
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_socket is not ISendSocket sendSocket) return Task.CompletedTask;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                sendSocket.Send(record.Value);

                // Handle request-reply patterns
                if (_socket is RequestSocket && _socket is IReceiveSocket reqReceiver)
                {
                    // Wait for reply (required for REQ/REP pattern)
                    try { reqReceiver.Receive(); } catch { }
                }
                else if (_socket is SurveyorSocket && _socket is IReceiveSocket surveyReceiver)
                {
                    // Collect survey responses
                    try
                    {
                        while (true)
                        {
                            var response = surveyReceiver.Receive();
                            if (response == null || response.Length == 0) break;
                        }
                    }
                    catch (NanomsgException)
                    {
                        // Survey complete or no more responses
                    }
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
