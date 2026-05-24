using System.Text;
using System.Text.Json;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune;

/// <summary>
/// Source task that executes Gremlin queries on AWS Neptune.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class NeptuneSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private GremlinClient? _client;
    private string _topic = string.Empty;
    private string _query = string.Empty;
    private int _pollIntervalMs;
    private DateTime _lastPoll = DateTime.MinValue;
    private long _offset;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var endpoint = config[NeptuneConnectorConfig.Endpoint];
        var port = config.TryGetValue(NeptuneConnectorConfig.Port, out var p) ? int.Parse(p) : NeptuneConnectorConfig.DefaultPort;
        var enableSsl = config.TryGetValue(NeptuneConnectorConfig.EnableSsl, out var ssl) && ssl == "true";

        _topic = config[NeptuneConnectorConfig.Topic];
        _query = config[NeptuneConnectorConfig.Query];
        _pollIntervalMs = config.TryGetValue(NeptuneConnectorConfig.PollIntervalMs, out var pi) ? int.Parse(pi) : NeptuneConnectorConfig.DefaultPollIntervalMs;

        var server = new GremlinServer(endpoint, port, enableSsl);
        _client = new GremlinClient(server, new GraphSON3MessageSerializer());
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Check poll interval
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(Math.Max(100, _pollIntervalMs - (int)(DateTime.UtcNow - _lastPoll).TotalMilliseconds), cancellationToken);
        }

        _lastPoll = DateTime.UtcNow;

        try
        {
            var results = await _client!.SubmitAsync<dynamic>(_query);

            foreach (var result in results)
            {
                var json = JsonSerializer.Serialize(result, JsonOptions);
                var value = Encoding.UTF8.GetBytes(json);
                var currentOffset = Interlocked.Increment(ref _offset);

                records.Add(new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object>
                    {
                        ["query"] = _query
                    },
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["offset"] = currentOffset
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes(currentOffset.ToString()),
                    Value = value,
                    Timestamp = DateTimeOffset.UtcNow,
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["neptune.query"] = Encoding.UTF8.GetBytes(_query)
                    }
                });
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    public override void Stop()
    {
        _client?.Dispose();
        _client = null;
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
#pragma warning restore CA2213
