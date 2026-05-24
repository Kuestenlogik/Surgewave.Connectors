using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Metrics;

/// <summary>
/// Metrics for MirrorMaker cross-cluster replication.
/// </summary>
public sealed class MirrorMetrics : IDisposable
{
    private readonly Meter _meter;

    // Counters
    private readonly Counter<long> _recordsReplicated;
    private readonly Counter<long> _bytesReplicated;
    private readonly Counter<long> _replicationErrors;
    private readonly Counter<long> _checkpointsEmitted;
    private readonly Counter<long> _heartbeatsEmitted;

    // Histograms
    private readonly Histogram<double> _replicationLatencyMs;

    // Gauges (via observable)
    private readonly ConcurrentDictionary<string, long> _replicationLag = new();
    private readonly ConcurrentDictionary<string, long> _lastReplicationTime = new();

    public MirrorMetrics(string sourceCluster, string targetCluster)
    {
        _meter = new Meter($"Kuestenlogik.Surgewave.Mirror.{sourceCluster}.{targetCluster}");

        _recordsReplicated = _meter.CreateCounter<long>(
            "mirror.records.replicated",
            description: "Number of records replicated");

        _bytesReplicated = _meter.CreateCounter<long>(
            "mirror.bytes.replicated",
            unit: "bytes",
            description: "Number of bytes replicated");

        _replicationErrors = _meter.CreateCounter<long>(
            "mirror.replication.errors",
            description: "Number of replication errors");

        _checkpointsEmitted = _meter.CreateCounter<long>(
            "mirror.checkpoints.emitted",
            description: "Number of checkpoints emitted");

        _heartbeatsEmitted = _meter.CreateCounter<long>(
            "mirror.heartbeats.emitted",
            description: "Number of heartbeats emitted");

        _replicationLatencyMs = _meter.CreateHistogram<double>(
            "mirror.replication.latency",
            unit: "ms",
            description: "Replication latency in milliseconds");

        _meter.CreateObservableGauge(
            "mirror.replication.lag",
            () => _replicationLag.Select(kv => new Measurement<long>(kv.Value, new KeyValuePair<string, object?>("partition", kv.Key))),
            description: "Replication lag in records");
    }

    public void RecordReplicated(string topic, int partition, int recordCount, int byteCount)
    {
        _recordsReplicated.Add(recordCount, new KeyValuePair<string, object?>("topic", topic));
        _bytesReplicated.Add(byteCount, new KeyValuePair<string, object?>("topic", topic));
        _lastReplicationTime[$"{topic}-{partition}"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public void RecordLatency(string topic, double latencyMs)
    {
        _replicationLatencyMs.Record(latencyMs, new KeyValuePair<string, object?>("topic", topic));
    }

    public void RecordError(string topic, string errorType)
    {
        _replicationErrors.Add(1,
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("error_type", errorType));
    }

    public void RecordCheckpoint()
    {
        _checkpointsEmitted.Add(1);
    }

    public void RecordHeartbeat()
    {
        _heartbeatsEmitted.Add(1);
    }

    public void UpdateLag(string topic, int partition, long lag)
    {
        _replicationLag[$"{topic}-{partition}"] = lag;
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}
