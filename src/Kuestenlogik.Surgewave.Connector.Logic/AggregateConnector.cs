using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Aggregates records over a time window.
/// Supports count, sum, avg, min, max operations.
/// </summary>
[ConnectorMetadata(
    Name = "Aggregate",
    Description = "Aggregate records over a time window (count, sum, avg, min, max)",
    Author = "Surgewave",
    Tags = "logic,aggregate,window,sum,count",
    Icon = "functions")]
public sealed class AggregateConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(AggregateTask);

    private string _groupByField = "";
    private string _aggregateField = "";
    private string _operation = "count";
    private int _windowMs = 60000;
    private string _outputTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(AggregateConfig.GroupByField, ConfigType.String, "", Importance.Medium,
            "JSON field to group by (optional, aggregates all if not specified)")
        .Define(AggregateConfig.AggregateField, ConfigType.String, "", Importance.Medium,
            "JSON field to aggregate (required for sum/avg/min/max, not for count)")
        .Define(AggregateConfig.Operation, ConfigType.String, "count", Importance.High,
            "Aggregation operation: count, sum, avg, min, max", EditorHint.Select, options: ["count", "sum", "avg", "min", "max"])
        .Define(AggregateConfig.WindowMs, ConfigType.Int, 60000, Importance.High,
            "Window size in milliseconds")
        .Define(AggregateConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send aggregated results to", EditorHint.Topic)
        .Define(AggregateConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic);

    public override void Start(IDictionary<string, string> config)
    {
        _groupByField = config.GetValueOrDefault(AggregateConfig.GroupByField, "") ?? "";
        _aggregateField = config.GetValueOrDefault(AggregateConfig.AggregateField, "") ?? "";
        _operation = (config.GetValueOrDefault(AggregateConfig.Operation, "count") ?? "count").ToLowerInvariant();
        _windowMs = int.TryParse(config.GetValueOrDefault(AggregateConfig.WindowMs, "60000"), out var w) ? w : 60000;
        _outputTopic = config.GetValueOrDefault(AggregateConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");

        var topics = config.GetValueOrDefault(AggregateConfig.Topics, "") ?? "";
        _inputTopics.AddRange(topics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        if (_inputTopics.Count == 0)
        {
            throw new ArgumentException("At least one input topic is required");
        }

        if (_operation != "count" && string.IsNullOrEmpty(_aggregateField))
        {
            throw new ArgumentException($"Aggregate field is required for {_operation} operation");
        }
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return
        [
            new Dictionary<string, string>
            {
                [AggregateConfig.GroupByField] = _groupByField,
                [AggregateConfig.AggregateField] = _aggregateField,
                [AggregateConfig.Operation] = _operation,
                [AggregateConfig.WindowMs] = _windowMs.ToString(),
                [AggregateConfig.OutputTopic] = _outputTopic,
                [AggregateConfig.Topics] = string.Join(",", _inputTopics)
            }
        ];
    }
}

/// <summary>
/// Task that performs aggregation.
/// </summary>
public sealed class AggregateTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _groupByField = "";
    private string _aggregateField = "";
    private string _operation = "count";
    private int _windowMs = 60000;
    private string _outputTopic = "";

    private readonly ConcurrentDictionary<string, AggregateState> _states = new();
    private Timer? _flushTimer;
    private readonly object _lock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _groupByField = config.GetValueOrDefault(AggregateConfig.GroupByField, "") ?? "";
        _aggregateField = config.GetValueOrDefault(AggregateConfig.AggregateField, "") ?? "";
        _operation = config.GetValueOrDefault(AggregateConfig.Operation, "count") ?? "count";
        _windowMs = int.TryParse(config.GetValueOrDefault(AggregateConfig.WindowMs, "60000"), out var w) ? w : 60000;
        _outputTopic = config.GetValueOrDefault(AggregateConfig.OutputTopic, "") ?? "";

        _flushTimer = new Timer(FlushCallback, null, _windowMs, _windowMs);
    }

    public override void Stop()
    {
        _flushTimer?.Dispose();
        _flushTimer = null;
        _states.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _flushTimer?.Dispose();
            _flushTimer = null;
        }
        base.Dispose(disposing);
    }

    private async void FlushCallback(object? state)
    {
        try
        {
            await FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        }
        catch
        {
            // Ignore flush errors
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            try
            {
                var (groupKey, value) = ExtractFields(record.Value);

                lock (_lock)
                {
                    var agg = _states.GetOrAdd(groupKey, _ => new AggregateState());
                    agg.Add(value);
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Aggregate failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }

        return Task.CompletedTask;
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (Context.Producer == null) return;

        Dictionary<string, AggregateState> snapshot;
        lock (_lock)
        {
            snapshot = new Dictionary<string, AggregateState>(_states);
            _states.Clear();
        }

        foreach (var (groupKey, agg) in snapshot)
        {
            var result = ComputeResult(agg);

            var output = new JsonObject
            {
                ["group"] = groupKey,
                ["operation"] = _operation,
                ["result"] = result,
                ["count"] = agg.Count,
                ["window_end"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            var outputBytes = Encoding.UTF8.GetBytes(output.ToJsonString());
            var keyBytes = Encoding.UTF8.GetBytes(groupKey);

            await Context.Producer.ProduceAsync(_outputTopic, keyBytes, outputBytes, cancellationToken);
        }
    }

    private (string groupKey, double? value) ExtractFields(byte[] data)
    {
        if (data.Length == 0) return ("_all", null);

        try
        {
            var json = JsonNode.Parse(data);
            if (json == null) return ("_all", null);

            // Extract group key
            string groupKey = "_all";
            if (!string.IsNullOrEmpty(_groupByField))
            {
                var keyNode = NavigatePath(json, _groupByField);
                groupKey = keyNode?.ToString() ?? "_null";
            }

            // Extract value
            double? value = null;
            if (!string.IsNullOrEmpty(_aggregateField))
            {
                var valueNode = NavigatePath(json, _aggregateField);
                if (valueNode != null && double.TryParse(valueNode.ToString(), out var d))
                {
                    value = d;
                }
            }

            return (groupKey, value);
        }
        catch
        {
            return ("_all", null);
        }
    }

    private static JsonNode? NavigatePath(JsonNode root, string path)
    {
        var current = root;
        foreach (var segment in path.Split('.'))
        {
            if (current is JsonObject obj && obj.TryGetPropertyValue(segment, out var next))
            {
                current = next;
            }
            else
            {
                return null;
            }
        }
        return current;
    }

    private double ComputeResult(AggregateState state)
    {
        return _operation switch
        {
            "count" => state.Count,
            "sum" => state.Sum,
            "avg" => state.Count > 0 ? state.Sum / state.Count : 0,
            "min" => state.Min,
            "max" => state.Max,
            _ => state.Count
        };
    }

    private sealed class AggregateState
    {
        public int Count { get; private set; }
        public double Sum { get; private set; }
        public double Min { get; private set; } = double.MaxValue;
        public double Max { get; private set; } = double.MinValue;

        public void Add(double? value)
        {
            Count++;
            if (value.HasValue)
            {
                Sum += value.Value;
                Min = Math.Min(Min, value.Value);
                Max = Math.Max(Max, value.Value);
            }
        }
    }
}

/// <summary>
/// Configuration keys for AggregateConnector.
/// </summary>
public static class AggregateConfig
{
    public const string GroupByField = "aggregate.group.by";
    public const string AggregateField = "aggregate.field";
    public const string Operation = "aggregate.operation";
    public const string WindowMs = "aggregate.window.ms";
    public const string OutputTopic = "output.topic";
    public const string Topics = "topics";
}
