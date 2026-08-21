using System.Text;
using System.Text.Json;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune;

/// <summary>
/// Sink task that writes vertices and edges to AWS Neptune.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed in Stop()
public sealed class NeptuneSinkTask : SinkTask
{
    private GremlinClient? _client;
    private string _writeMode = string.Empty;
    private string _vertexLabel = string.Empty;
    private string _edgeLabel = string.Empty;
    private string _idField = string.Empty;
    private string _fromField = string.Empty;
    private string _toField = string.Empty;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var endpoint = config[NeptuneConnectorConfig.Endpoint];
        var port = config.TryGetValue(NeptuneConnectorConfig.Port, out var p) ? int.Parse(p) : NeptuneConnectorConfig.DefaultPort;
        var enableSsl = config.TryGetValue(NeptuneConnectorConfig.EnableSsl, out var ssl) && ssl == "true";

        ApplyConfiguration(config);

        var server = new GremlinServer(endpoint, port, enableSsl);
        _client = new GremlinClient(server, new GraphSON3MessageSerializer());
    }

    /// <summary>
    /// Applies the write mode, labels and field names from the connector configuration.
    /// Split out of <see cref="Start"/> so the query builders can be exercised without a live client.
    /// </summary>
    internal void ApplyConfiguration(IDictionary<string, string> config)
    {
        _writeMode = config.TryGetValue(NeptuneConnectorConfig.WriteMode, out var wm) ? wm : NeptuneConnectorConfig.DefaultWriteMode;
        _vertexLabel = config.TryGetValue(NeptuneConnectorConfig.VertexLabel, out var vl) ? vl : "vertex";
        _edgeLabel = config.TryGetValue(NeptuneConnectorConfig.EdgeLabel, out var el) ? el : "edge";
        _idField = config.TryGetValue(NeptuneConnectorConfig.IdField, out var idf) ? idf : "id";
        _fromField = config.TryGetValue(NeptuneConnectorConfig.FromField, out var ff) ? ff : "from";
        _toField = config.TryGetValue(NeptuneConnectorConfig.ToField, out var tf) ? tf : "to";
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, object>>(json);
                if (data == null) continue;

                if (_writeMode == "edge")
                {
                    await WriteEdgeAsync(data);
                }
                else
                {
                    await WriteVertexAsync(data);
                }
            }
            catch (Exception ex)
            {
                // Rethrow so the worker retries and eventually routes to the DLQ
                // instead of committing offsets for records that were never written.
                Context?.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    private async Task WriteVertexAsync(Dictionary<string, object> data)
    {
        await _client!.SubmitAsync<dynamic>(BuildVertexQuery(data));
    }

    private async Task WriteEdgeAsync(Dictionary<string, object> data)
    {
        var query = BuildEdgeQuery(data);
        if (query == null)
            return;

        await _client!.SubmitAsync<dynamic>(query);
    }

    /// <summary>
    /// Builds the Gremlin script that adds a vertex for the given record.
    /// Internal so the escaping of labels, ids and property keys is testable without a live client.
    /// </summary>
    internal string BuildVertexQuery(Dictionary<string, object> data)
    {
        data.TryGetValue(_idField, out var idObj);
        var id = idObj?.ToString() ?? Guid.NewGuid().ToString();
        var properties = BuildProperties(data, _idField);

        return $"g.addV('{EscapeGremlin(_vertexLabel)}').property('id', '{EscapeGremlin(id)}'){properties}";
    }

    /// <summary>
    /// Builds the Gremlin script that adds an edge for the given record, or <c>null</c> when the
    /// record carries no source or target vertex. Internal so the escaping is testable.
    /// </summary>
    internal string? BuildEdgeQuery(Dictionary<string, object> data)
    {
        data.TryGetValue(_fromField, out var fromObj);
        data.TryGetValue(_toField, out var toObj);
        var fromId = fromObj?.ToString();
        var toId = toObj?.ToString();

        if (string.IsNullOrEmpty(fromId) || string.IsNullOrEmpty(toId))
            return null;

        var properties = BuildProperties(data, _idField, _fromField, _toField);

        return $"g.V('{EscapeGremlin(fromId)}').addE('{EscapeGremlin(_edgeLabel)}').to(g.V('{EscapeGremlin(toId)}')){properties}";
    }

    private static string BuildProperties(Dictionary<string, object> data, params string[] excludeFields)
    {
        var exclude = new HashSet<string>(excludeFields, StringComparer.OrdinalIgnoreCase);
        var sb = new StringBuilder();

        foreach (var (key, value) in data)
        {
            if (exclude.Contains(key) || value == null) continue;

            var escapedValue = EscapeGremlin(value.ToString() ?? "");
            sb.Append($".property('{EscapeGremlin(key)}', '{escapedValue}')");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Escapes a value for embedding in a single-quoted Gremlin string literal.
    /// Backslashes first, then quotes, so no new escape sequences are introduced.
    /// </summary>
    private static string EscapeGremlin(string value)
        => value.Replace("\\", "\\\\").Replace("'", "\\'");

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
