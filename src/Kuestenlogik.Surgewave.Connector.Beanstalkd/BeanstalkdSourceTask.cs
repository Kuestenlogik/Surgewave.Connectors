using System.Diagnostics.CodeAnalysis;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Task that reserves jobs from a Beanstalkd tube and produces source records.
/// Supports manual deletion after successful commit for at-least-once semantics.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class BeanstalkdSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _host = BeanstalkdConnectorConfig.DefaultHost;
    private int _port = BeanstalkdConnectorConfig.DefaultPort;
    private string _tube = BeanstalkdConnectorConfig.DefaultTube;
    private string _topic = "";
    private int _reserveTimeoutSeconds = BeanstalkdConnectorConfig.DefaultReserveTimeoutSeconds;
    private int _batchSize = BeanstalkdConnectorConfig.DefaultBatchSize;
    private int _pollTimeoutMs = BeanstalkdConnectorConfig.DefaultPollTimeoutMs;

    private BeanstalkClient? _client;
    private CancellationTokenSource? _cts;
    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly List<long> _pendingJobIds = [];

    public override void Start(IDictionary<string, string> config)
    {
        _host = config.TryGetValue(BeanstalkdConnectorConfig.Host, out var host)
            ? host : BeanstalkdConnectorConfig.DefaultHost;
        _port = config.TryGetValue(BeanstalkdConnectorConfig.Port, out var port)
            ? int.Parse(port) : BeanstalkdConnectorConfig.DefaultPort;
        _tube = config.TryGetValue(BeanstalkdConnectorConfig.Tube, out var tube)
            ? tube : BeanstalkdConnectorConfig.DefaultTube;
        _topic = config.TryGetValue(BeanstalkdConnectorConfig.Topic, out var topic)
            ? topic : "";

        if (config.TryGetValue(BeanstalkdConnectorConfig.ReserveTimeoutSeconds, out var timeout))
            _reserveTimeoutSeconds = int.Parse(timeout);
        if (config.TryGetValue(BeanstalkdConnectorConfig.BatchSize, out var bs))
            _batchSize = int.Parse(bs);
        if (config.TryGetValue(BeanstalkdConnectorConfig.PollTimeoutMs, out var pt))
            _pollTimeoutMs = int.Parse(pt);

        _sourcePartition["connector"] = "beanstalkd";
        _sourcePartition["host"] = _host;
        _sourcePartition["port"] = _port;
        _sourcePartition["tube"] = _tube;

        _cts = new CancellationTokenSource();
        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        _client = new BeanstalkClient(_host, _port);
        await _client.ConnectAsync();

        // Watch the configured tube
        await _client.WatchAsync(_tube);

        // Ignore the default tube if we're watching a different one
        if (_tube != "default")
        {
            await _client.IgnoreAsync("default");
        }
    }

    public override void Stop()
    {
        _cts?.Cancel();

        _client?.Dispose();
        _client = null;

        _cts?.Dispose();
        _cts = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (_client == null)
            return records;

        using var timeoutCts = new CancellationTokenSource(_pollTimeoutMs);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            while (records.Count < _batchSize && !linkedCts.Token.IsCancellationRequested)
            {
                // Reserve a job with timeout
                var job = await _client.ReserveAsync(TimeSpan.FromSeconds(_reserveTimeoutSeconds));

                if (job == null)
                {
                    // No job available within timeout
                    break;
                }

                var record = CreateSourceRecord(job);
                records.Add(record);
                _pendingJobIds.Add(job.Id);
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout or cancellation - return what we have
        }
        catch (Exception)
        {
            // Connection issue or other error - return what we have
        }

        return records;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_client == null || _pendingJobIds.Count == 0)
            return;

        // Delete all successfully processed jobs
        foreach (var jobId in _pendingJobIds)
        {
            try
            {
                await _client.DeleteAsync(jobId);
            }
            catch (Exception)
            {
                // Job may have been deleted already or connection issue
                // Continue with other jobs
            }
        }

        _pendingJobIds.Clear();
    }

    private SourceRecord CreateSourceRecord(BeanstalkJob job)
    {
        var sourceOffset = new Dictionary<string, object>
        {
            [BeanstalkdConnectorConfig.OffsetJobId] = job.Id,
            [BeanstalkdConnectorConfig.OffsetTimestamp] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Use job ID as the message key
        var key = Encoding.UTF8.GetBytes(job.Id.ToString());

        var headers = new Dictionary<string, byte[]>
        {
            ["beanstalkd.job.id"] = BitConverter.GetBytes(job.Id),
            ["beanstalkd.tube"] = Encoding.UTF8.GetBytes(_tube)
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = key,
            Value = job.Data,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = DateTimeOffset.UtcNow,
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
