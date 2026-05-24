using System.Collections.Concurrent;
using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// Task that consumes messages from NATS JetStream and produces them as records.
/// </summary>
public sealed class NatsSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _natsUrl = NatsConnectorConfig.DefaultUrl;
    private string _streamName = "";
    private string _consumerName = "";
    private bool _durable = NatsConnectorConfig.DefaultConsumerDurable;
    private string _deliverPolicy = NatsConnectorConfig.DefaultDeliverPolicy;
    private string _ackPolicy = NatsConnectorConfig.DefaultAckPolicy;
    private int _maxAckPending = NatsConnectorConfig.DefaultMaxAckPending;
    private int _fetchBatchSize = NatsConnectorConfig.DefaultFetchBatchSize;
    private int _fetchTimeoutMs = NatsConnectorConfig.DefaultFetchTimeoutMs;
    private string? _credentialsFile;
    private string? _token;
    private string? _username;
    private string? _password;

    private NatsConnection? _connection;
    private NatsJSContext? _jetStream;
    private INatsJSConsumer? _consumer;
    private readonly ConcurrentQueue<PendingMessage> _pendingAcks = new();

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[NatsConnectorConfig.Topic];
        
        if (config.TryGetValue(NatsConnectorConfig.Url, out var url))
            _natsUrl = url;
        _streamName = config[NatsConnectorConfig.StreamName];
        _consumerName = config[NatsConnectorConfig.ConsumerName];
        
        if (config.TryGetValue(NatsConnectorConfig.ConsumerDurable, out var durable))
            _durable = bool.Parse(durable);
        if (config.TryGetValue(NatsConnectorConfig.DeliverPolicy, out var deliverPolicy))
            _deliverPolicy = deliverPolicy;
        if (config.TryGetValue(NatsConnectorConfig.AckPolicy, out var ackPolicy))
            _ackPolicy = ackPolicy;
        if (config.TryGetValue(NatsConnectorConfig.MaxAckPending, out var maxAckPending))
            _maxAckPending = int.Parse(maxAckPending);
        if (config.TryGetValue(NatsConnectorConfig.FetchBatchSize, out var fetchBatchSize))
            _fetchBatchSize = int.Parse(fetchBatchSize);
        if (config.TryGetValue(NatsConnectorConfig.FetchTimeoutMs, out var fetchTimeout))
            _fetchTimeoutMs = int.Parse(fetchTimeout);
        if (config.TryGetValue(NatsConnectorConfig.CredentialsFile, out var creds))
            _credentialsFile = creds;
        if (config.TryGetValue(NatsConnectorConfig.Token, out var token))
            _token = token;
        if (config.TryGetValue(NatsConnectorConfig.Username, out var username))
            _username = username;
        if (config.TryGetValue(NatsConnectorConfig.Password, out var password))
            _password = password;

        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        var opts = new NatsOpts
        {
            Url = _natsUrl
        };

        if (!string.IsNullOrEmpty(_credentialsFile))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = _credentialsFile } };
        }
        else if (!string.IsNullOrEmpty(_token))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Token = _token } };
        }
        else if (!string.IsNullOrEmpty(_username) && !string.IsNullOrEmpty(_password))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Username = _username, Password = _password } };
        }

        _connection = new NatsConnection(opts);
        await _connection.ConnectAsync();

        _jetStream = new NatsJSContext(_connection);

        var deliverPolicy = _deliverPolicy.ToLowerInvariant() switch
        {
            "all" => ConsumerConfigDeliverPolicy.All,
            "last" => ConsumerConfigDeliverPolicy.Last,
            "new" => ConsumerConfigDeliverPolicy.New,
            "by_start_sequence" => ConsumerConfigDeliverPolicy.ByStartSequence,
            "by_start_time" => ConsumerConfigDeliverPolicy.ByStartTime,
            "last_per_subject" => ConsumerConfigDeliverPolicy.LastPerSubject,
            _ => ConsumerConfigDeliverPolicy.All
        };

        var ackPolicy = _ackPolicy.ToLowerInvariant() switch
        {
            "explicit" => ConsumerConfigAckPolicy.Explicit,
            "none" => ConsumerConfigAckPolicy.None,
            "all" => ConsumerConfigAckPolicy.All,
            _ => ConsumerConfigAckPolicy.Explicit
        };

        var consumerConfig = new ConsumerConfig
        {
            Name = _consumerName,
            DurableName = _durable ? _consumerName : null,
            DeliverPolicy = deliverPolicy,
            AckPolicy = ackPolicy,
            MaxAckPending = _maxAckPending
        };

        _consumer = await _jetStream.CreateOrUpdateConsumerAsync(_streamName, consumerConfig);
    }

    public override void Stop()
    {
        // Ack any remaining pending messages
        while (_pendingAcks.TryDequeue(out var pending))
        {
            try { pending.AckAsync().AsTask().GetAwaiter().GetResult(); } catch { /* ignore */ }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try { _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* ignore */ }
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_consumer == null)
            return [];

        var records = new List<SourceRecord>();

        try
        {
            using var fetchCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            fetchCts.CancelAfter(_fetchTimeoutMs);

            await foreach (var msg in _consumer.FetchAsync<byte[]>(
                new NatsJSFetchOpts { MaxMsgs = _fetchBatchSize },
                cancellationToken: fetchCts.Token))
            {
                var sourcePartition = new Dictionary<string, object>
                {
                    ["stream"] = _streamName,
                    ["consumer"] = _consumerName
                };

                var sourceOffset = new Dictionary<string, object>
                {
                    [NatsConnectorConfig.OffsetStreamSequence] = msg.Metadata?.Sequence.Stream ?? 0,
                    [NatsConnectorConfig.OffsetConsumerSequence] = msg.Metadata?.Sequence.Consumer ?? 0
                };

                var record = new SourceRecord
                {
                    SourcePartition = sourcePartition,
                    SourceOffset = sourceOffset,
                    Topic = _topic,
                    Key = msg.Subject != null ? Encoding.UTF8.GetBytes(msg.Subject) : null,
                    Value = msg.Data ?? [],
                    Timestamp = msg.Metadata?.Timestamp ?? DateTimeOffset.UtcNow
                };

                records.Add(record);
                _pendingAcks.Enqueue(new PendingMessage(msg));
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Fetch timeout - normal operation
        }

        return records;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        while (_pendingAcks.TryDequeue(out var pending))
        {
            await pending.AckAsync(cancellationToken);
        }
    }

    private sealed class PendingMessage
    {
        private readonly INatsJSMsg<byte[]> _msg;

        public PendingMessage(INatsJSMsg<byte[]> msg)
        {
            _msg = msg;
        }

        public ValueTask AckAsync(CancellationToken ct = default)
        {
            return _msg.AckAsync(cancellationToken: ct);
        }
    }
}
