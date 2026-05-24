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

        _writeMode = config.TryGetValue(NeptuneConnectorConfig.WriteMode, out var wm) ? wm : NeptuneConnectorConfig.DefaultWriteMode;
        _vertexLabel = config.TryGetValue(NeptuneConnectorConfig.VertexLabel, out var vl) ? vl : "vertex";
        _edgeLabel = config.TryGetValue(NeptuneConnectorConfig.EdgeLabel, out var el) ? el : "edge";
        _idField = config.TryGetValue(NeptuneConnectorConfig.IdField, out var idf) ? idf : "id";
        _fromField = config.TryGetValue(NeptuneConnectorConfig.FromField, out var ff) ? ff : "from";
        _toField = config.TryGetValue(NeptuneConnectorConfig.ToField, out var tf) ? tf : "to";

        var server = new GremlinServer(endpoint, port, enableSsl);
        _client = new GremlinClient(server, new GraphSON3MessageSerializer());
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
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private async Task WriteVertexAsync(Dictionary<string, object> data)
    {
        data.TryGetValue(_idField, out var idObj);
        var id = idObj?.ToString() ?? Guid.NewGuid().ToString();
        var properties = BuildProperties(data, _idField);

        var query = $"g.addV('{_vertexLabel}').property('id', '{id}'){properties}";
        await _client!.SubmitAsync<dynamic>(query);
    }

    private async Task WriteEdgeAsync(Dictionary<string, object> data)
    {
        data.TryGetValue(_fromField, out var fromObj);
        data.TryGetValue(_toField, out var toObj);
        var fromId = fromObj?.ToString();
        var toId = toObj?.ToString();

        if (string.IsNullOrEmpty(fromId) || string.IsNullOrEmpty(toId))
            return;

        var properties = BuildProperties(data, _idField, _fromField, _toField);

        var query = $"g.V('{fromId}').addE('{_edgeLabel}').to(g.V('{toId}')){properties}";
        await _client!.SubmitAsync<dynamic>(query);
    }

    private static string BuildProperties(Dictionary<string, object> data, params string[] excludeFields)
    {
        var exclude = new HashSet<string>(excludeFields, StringComparer.OrdinalIgnoreCase);
        var sb = new StringBuilder();

        foreach (var (key, value) in data)
        {
            if (exclude.Contains(key) || value == null) continue;

            var escapedValue = value.ToString()?.Replace("'", "\\'") ?? "";
            sb.Append($".property('{key}', '{escapedValue}')");
        }

        return sb.ToString();
    }

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
