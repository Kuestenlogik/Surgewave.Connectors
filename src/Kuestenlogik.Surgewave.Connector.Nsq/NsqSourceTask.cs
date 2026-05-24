using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading.Channels;
using NsqSharp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nsq;

/// <summary>
/// Task that consumes messages from NSQ using NsqSharp Consumer and produces source records.
/// Supports both direct nsqd connections and nsqlookupd discovery.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class NsqSourceTask : SourceTask, IHandler
{
    public override string Version => "1.0.0";

    private string _nsqdAddress = NsqConnectorConfig.DefaultNsqdAddress;
    private string _nsqLookupdAddresses = "";
    private string _nsqTopic = "";
    private string _nsqChannel = NsqConnectorConfig.DefaultChannel;
    private string _topic = "";
    private int _maxInFlight = NsqConnectorConfig.DefaultMaxInFlight;
    private int _maxAttempts = NsqConnectorConfig.DefaultMaxAttempts;
    private int _batchSize = NsqConnectorConfig.DefaultBatchSize;
    private int _pollTimeoutMs = NsqConnectorConfig.DefaultPollTimeoutMs;
    private int _requeueDelayMs = NsqConnectorConfig.DefaultRequeueDelayMs;

    private Consumer? _consumer;
    private CancellationTokenSource? _cts;
    private readonly Channel<IMessage> _messageChannel = Channel.CreateBounded<IMessage>(1000);
    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<IMessage> _pendingMessages = [];

    public override void Start(IDictionary<string, string> config)
    {
        _nsqdAddress = config.TryGetValue(NsqConnectorConfig.NsqdAddress, out var nsqd) ? nsqd : NsqConnectorConfig.DefaultNsqdAddress;
        _nsqLookupdAddresses = config.TryGetValue(NsqConnectorConfig.NsqLookupdAddresses, out var lookupd) ? lookupd : "";
        _nsqTopic = config.TryGetValue(NsqConnectorConfig.NsqTopic, out var nsqTopic) ? nsqTopic : "";
        _nsqChannel = config.TryGetValue(NsqConnectorConfig.NsqChannel, out var channel) ? channel : NsqConnectorConfig.DefaultChannel;
        _topic = config.TryGetValue(NsqConnectorConfig.Topic, out var t) ? t : "";

        if (config.TryGetValue(NsqConnectorConfig.MaxInFlight, out var mif))
            _maxInFlight = int.Parse(mif);
        if (config.TryGetValue(NsqConnectorConfig.MaxAttempts, out var ma))
            _maxAttempts = int.Parse(ma);
        if (config.TryGetValue(NsqConnectorConfig.BatchSize, out var bs))
            _batchSize = int.Parse(bs);
        if (config.TryGetValue(NsqConnectorConfig.PollTimeoutMs, out var pt))
            _pollTimeoutMs = int.Parse(pt);
        if (config.TryGetValue(NsqConnectorConfig.RequeueDelayMs, out var rd))
            _requeueDelayMs = int.Parse(rd);

        _sourcePartition["connector"] = "nsq";
        _sourcePartition["topic"] = _nsqTopic;
        _sourcePartition["channel"] = _nsqChannel;

        _cts = new CancellationTokenSource();

        // Create NSQ consumer
        var nsqConfig = new Config
        {
            MaxInFlight = _maxInFlight,
            MaxAttempts = (ushort)_maxAttempts,
            DefaultRequeueDelay = TimeSpan.FromMilliseconds(_requeueDelayMs)
        };

        // Configure auth if provided
        if (config.TryGetValue(NsqConnectorConfig.AuthSecret, out var authSecret) && !string.IsNullOrEmpty(authSecret))
        {
            nsqConfig.AuthSecret = authSecret;
        }

        _consumer = new Consumer(_nsqTopic, _nsqChannel, nsqConfig);
        _consumer.AddHandler(this);

        // Connect using nsqlookupd or direct nsqd
        if (!string.IsNullOrEmpty(_nsqLookupdAddresses))
        {
            var lookupdAddresses = _nsqLookupdAddresses.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            _consumer.ConnectToNsqLookupd(lookupdAddresses);
        }
        else if (!string.IsNullOrEmpty(_nsqdAddress))
        {
            _consumer.ConnectToNsqd(_nsqdAddress);
        }
    }

    /// <summary>
    /// IHandler implementation - receives messages from NSQ.
    /// </summary>
    public void HandleMessage(IMessage message)
    {
        // Queue the message for processing by PollAsync
        // This blocks if the channel is full, applying backpressure
        _messageChannel.Writer.TryWrite(message);
    }

    /// <summary>
    /// Called to log messages from NSQ client.
    /// </summary>
    public void LogFailedMessage(IMessage message)
    {
        // Message reached max attempts - could log or handle specially
    }

    public override void Stop()
    {
        _cts?.Cancel();

        // Finish any pending messages by requeuing them
        foreach (var msg in _pendingMessages)
        {
            try { msg.Requeue(TimeSpan.FromMilliseconds(_requeueDelayMs)); } catch { /* ignore */ }
        }
        _pendingMessages.Clear();

        _consumer?.Stop();
        _consumer = null;

        _cts?.Dispose();
        _cts = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        using var timeoutCts = new CancellationTokenSource(_pollTimeoutMs);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (records.Count < _batchSize)
            {
                if (_messageChannel.Reader.TryRead(out var message))
                {
                    var record = CreateSourceRecord(message);
                    records.Add(record);
                    _pendingMessages.Add(message);
                }
                else
                {
                    var hasMore = await _messageChannel.Reader.WaitToReadAsync(linkedCts.Token);
                    if (!hasMore)
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout or cancellation - return what we have
        }

        return records;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // Acknowledge all pending messages
        foreach (var msg in _pendingMessages)
        {
            try { msg.Finish(); } catch { /* ignore */ }
        }
        _pendingMessages.Clear();

        return Task.CompletedTask;
    }

    private SourceRecord CreateSourceRecord(IMessage message)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            [NsqConnectorConfig.OffsetTimestamp] = message.Timestamp.Ticks,
            [NsqConnectorConfig.OffsetAttempts] = message.Attempts,
            [NsqConnectorConfig.OffsetMessageId] = message.Id
        };

        var headers = new Dictionary<string, byte[]>
        {
            ["nsq.topic"] = Encoding.UTF8.GetBytes(_nsqTopic),
            ["nsq.channel"] = Encoding.UTF8.GetBytes(_nsqChannel),
            ["nsq.message.id"] = Encoding.UTF8.GetBytes(message.Id),
            ["nsq.timestamp"] = BitConverter.GetBytes(message.Timestamp.Ticks),
            ["nsq.attempts"] = BitConverter.GetBytes((int)message.Attempts)
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(message.Id),
            Value = message.Body,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = message.Timestamp,
            Headers = headers
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
