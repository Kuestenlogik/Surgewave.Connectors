using System.Diagnostics.CodeAnalysis;
using System.Text;
using NsqSharp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nsq;

/// <summary>
/// Task that publishes sink records to NSQ using NsqSharp Producer.
/// Supports retries and timeout configuration.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class NsqSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _nsqdAddress = NsqConnectorConfig.DefaultNsqdAddress;
    private string _nsqTopic = "";
    private int _publishTimeoutMs = NsqConnectorConfig.DefaultPublishTimeoutMs;
    private int _retries = NsqConnectorConfig.DefaultRetries;

    private Producer? _producer;
    private readonly object _lock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _nsqdAddress = config.TryGetValue(NsqConnectorConfig.NsqdAddress, out var nsqd) ? nsqd : NsqConnectorConfig.DefaultNsqdAddress;
        _nsqTopic = config.TryGetValue(NsqConnectorConfig.NsqTopic, out var nsqTopic) ? nsqTopic : "";

        if (config.TryGetValue(NsqConnectorConfig.PublishTimeoutMs, out var pt))
            _publishTimeoutMs = int.Parse(pt);
        if (config.TryGetValue(NsqConnectorConfig.Retries, out var retries))
            _retries = int.Parse(retries);

        // Create NSQ producer configuration
        var nsqConfig = new Config();

        // Configure auth if provided
        if (config.TryGetValue(NsqConnectorConfig.AuthSecret, out var authSecret) && !string.IsNullOrEmpty(authSecret))
        {
            nsqConfig.AuthSecret = authSecret;
        }

        _producer = new Producer(_nsqdAddress, nsqConfig);
    }

    public override void Stop()
    {
        lock (_lock)
        {
            _producer?.Stop();
            _producer = null;
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _producer == null)
            return;

        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var body = record.Value ?? [];
            if (body.Length == 0)
                continue;

            var lastException = default(Exception);
            for (var attempt = 0; attempt <= _retries; attempt++)
            {
                try
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeoutCts.CancelAfter(_publishTimeoutMs);

                    // NsqSharp Producer.Publish is synchronous, so we run it on a thread pool thread
                    await Task.Run(() =>
                    {
                        lock (_lock)
                        {
                            _producer?.Publish(_nsqTopic, body);
                        }
                    }, timeoutCts.Token);

                    lastException = null;
                    break;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested && attempt < _retries)
                {
                    lastException = ex;
                    await Task.Delay(100 * (attempt + 1), cancellationToken);
                }
            }

            if (lastException != null)
            {
                throw new InvalidOperationException(
                    $"Failed to publish message to NSQ topic '{_nsqTopic}' after {_retries + 1} attempts",
                    lastException);
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // NSQ publishes are immediately sent to nsqd
        return Task.CompletedTask;
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
