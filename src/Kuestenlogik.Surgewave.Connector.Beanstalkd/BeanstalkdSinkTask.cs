using System.Diagnostics.CodeAnalysis;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Task that puts sink records as jobs into a Beanstalkd tube.
/// Supports configurable priority, delay, and time-to-run.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class BeanstalkdSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _host = BeanstalkdConnectorConfig.DefaultHost;
    private int _port = BeanstalkdConnectorConfig.DefaultPort;
    private string _tube = BeanstalkdConnectorConfig.DefaultTube;
    private uint _priority = BeanstalkdConnectorConfig.DefaultPriority;
    private int _delaySeconds = BeanstalkdConnectorConfig.DefaultDelaySeconds;
    private int _ttrSeconds = BeanstalkdConnectorConfig.DefaultTtrSeconds;

    private BeanstalkClient? _client;
    private CancellationTokenSource? _cts;
    private readonly object _lock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _host = config.TryGetValue(BeanstalkdConnectorConfig.Host, out var host)
            ? host : BeanstalkdConnectorConfig.DefaultHost;
        _port = config.TryGetValue(BeanstalkdConnectorConfig.Port, out var port)
            ? int.Parse(port) : BeanstalkdConnectorConfig.DefaultPort;
        _tube = config.TryGetValue(BeanstalkdConnectorConfig.Tube, out var tube)
            ? tube : BeanstalkdConnectorConfig.DefaultTube;

        if (config.TryGetValue(BeanstalkdConnectorConfig.Priority, out var priority))
            _priority = uint.Parse(priority);
        if (config.TryGetValue(BeanstalkdConnectorConfig.DelaySeconds, out var delay))
            _delaySeconds = int.Parse(delay);
        if (config.TryGetValue(BeanstalkdConnectorConfig.TtrSeconds, out var ttr))
            _ttrSeconds = int.Parse(ttr);

        _cts = new CancellationTokenSource();
        ConnectAsync().GetAwaiter().GetResult();
    }

    private async Task ConnectAsync()
    {
        _client = new BeanstalkClient(_host, _port);
        await _client.ConnectAsync();

        // Use the configured tube
        await _client.UseAsync(_tube);
    }

    public override void Stop()
    {
        lock (_lock)
        {
            _cts?.Cancel();

            _client?.Dispose();
            _client = null;

            _cts?.Dispose();
            _cts = null;
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _client == null)
            return;

        var delay = TimeSpan.FromSeconds(_delaySeconds);
        var ttr = TimeSpan.FromSeconds(_ttrSeconds);

        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var jobData = record.Value ?? [];

            // Put the job into the tube
            await _client.PutAsync(jobData, _priority, delay, ttr);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Beanstalkd put operations are synchronous - no buffering needed
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
