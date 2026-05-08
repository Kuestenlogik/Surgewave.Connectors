using System.Text;
using System.Text.Json;
using GraphQL;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.SystemTextJson;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL;

/// <summary>
/// Task that sends records to a GraphQL API as mutations.
/// </summary>
public sealed class GraphQLSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private GraphQLHttpClient? _client;
    private string _mutation = "";
    private string _operationName = "";
    private Dictionary<string, string> _variablesMapping = [];
    private int _batchSize = GraphQLConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = GraphQLConnectorConfig.DefaultMaxRetryCount;
    private int _retryDelayMs = GraphQLConnectorConfig.DefaultRetryDelayMs;
    private readonly List<SinkRecord> _buffer = [];

    public override void Start(IDictionary<string, string> config)
    {
        var endpoint = config[GraphQLConnectorConfig.EndpointConfig];
        _mutation = config[GraphQLConnectorConfig.MutationConfig];

        _operationName = config.TryGetValue(GraphQLConnectorConfig.OperationNameConfig, out var opName)
            ? opName : "";
        _batchSize = config.TryGetValue(GraphQLConnectorConfig.BatchSizeConfig, out var batchSize)
            ? int.Parse(batchSize) : GraphQLConnectorConfig.DefaultBatchSize;
        _maxRetryCount = config.TryGetValue(GraphQLConnectorConfig.MaxRetryCountConfig, out var maxRetry)
            ? int.Parse(maxRetry) : GraphQLConnectorConfig.DefaultMaxRetryCount;
        _retryDelayMs = config.TryGetValue(GraphQLConnectorConfig.RetryDelayMsConfig, out var retryDelay)
            ? int.Parse(retryDelay) : GraphQLConnectorConfig.DefaultRetryDelayMs;

        var timeoutMs = config.TryGetValue(GraphQLConnectorConfig.TimeoutMsConfig, out var timeout)
            ? int.Parse(timeout) : GraphQLConnectorConfig.DefaultTimeoutMs;

        // Parse variable mapping
        if (config.TryGetValue(GraphQLConnectorConfig.VariablesMappingConfig, out var mapping) &&
            !string.IsNullOrWhiteSpace(mapping))
        {
            foreach (var pair in mapping.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = pair.Split('=', 2);
                if (parts.Length == 2)
                {
                    _variablesMapping[parts[0].Trim()] = parts[1].Trim();
                }
            }
        }

        // Create HTTP client
        _client = new GraphQLHttpClient(endpoint, new SystemTextJsonSerializer());
        _client.HttpClient.Timeout = TimeSpan.FromMilliseconds(timeoutMs);

        // Add auth header
        var authHeader = config.TryGetValue(GraphQLConnectorConfig.AuthHeaderConfig, out var ah)
            ? ah : GraphQLConnectorConfig.DefaultAuthHeader;
        if (config.TryGetValue(GraphQLConnectorConfig.AuthTokenConfig, out var authToken) &&
            !string.IsNullOrWhiteSpace(authToken))
        {
            _client.HttpClient.DefaultRequestHeaders.Add(authHeader, authToken);
        }

        // Add custom headers
        if (config.TryGetValue(GraphQLConnectorConfig.HeadersConfig, out var headers) &&
            !string.IsNullOrWhiteSpace(headers))
        {
            foreach (var header in headers.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = header.Split('=', 2);
                if (parts.Length == 2)
                {
                    _client.HttpClient.DefaultRequestHeaders.TryAddWithoutValidation(
                        parts[0].Trim(), parts[1].Trim());
                }
            }
        }
    }

    public override void Stop()
    {
        FlushBuffer();
        _client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _buffer.Clear();
            _client?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null || records.Count == 0)
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            _buffer.Add(record);

            if (_buffer.Count >= _batchSize)
            {
                await FlushBufferAsync(cancellationToken);
            }
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBufferAsync(cancellationToken);
    }

    private void FlushBuffer() => FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0 || _client == null)
            return;

        // Check if mutation supports batching ($inputs array)
        if (_mutation.Contains("$inputs", StringComparison.OrdinalIgnoreCase))
        {
            await ExecuteBatchMutationAsync(_buffer, cancellationToken);
        }
        else
        {
            // Execute individual mutations
            foreach (var record in _buffer)
            {
                await ExecuteSingleMutationAsync(record, cancellationToken);
            }
        }

        _buffer.Clear();
    }

    private async Task ExecuteSingleMutationAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var variables = BuildVariables(record);
        await ExecuteWithRetryAsync(variables, cancellationToken);
    }

    private async Task ExecuteBatchMutationAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        var inputs = new List<object?>();

        foreach (var record in records)
        {
            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var element = JsonSerializer.Deserialize<JsonElement>(json);
                inputs.Add(element);
            }
            catch (JsonException)
            {
                // Skip invalid JSON records
            }
        }

        if (inputs.Count == 0)
            return;

        var variables = new Dictionary<string, object?>
        {
            ["inputs"] = inputs
        };

        await ExecuteWithRetryAsync(variables, cancellationToken);
    }

    private Dictionary<string, object?> BuildVariables(SinkRecord record)
    {
        var variables = new Dictionary<string, object?>();

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            var doc = JsonDocument.Parse(json);

            if (_variablesMapping.Count > 0)
            {
                // Map specific fields
                foreach (var (varName, jsonPath) in _variablesMapping)
                {
                    var value = ExtractValue(doc.RootElement, jsonPath);
                    variables[varName] = value;
                }
            }
            else
            {
                // Pass entire record as "input" variable
                variables["input"] = doc.RootElement;
            }
        }
        catch (JsonException)
        {
            // If not valid JSON, pass raw value as string
            variables["input"] = Encoding.UTF8.GetString(record.Value);
        }

        return variables;
    }

    private static object? ExtractValue(JsonElement element, string jsonPath)
    {
        var current = element;

        foreach (var part in jsonPath.Split('.'))
        {
            if (current.TryGetProperty(part, out var next))
                current = next;
            else
                return null;
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString(),
            JsonValueKind.Number => current.TryGetInt64(out var l) ? l : current.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => current
        };
    }

    private async Task ExecuteWithRetryAsync(Dictionary<string, object?> variables, CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                var request = new GraphQLRequest
                {
                    Query = _mutation,
                    OperationName = string.IsNullOrWhiteSpace(_operationName) ? null : _operationName,
                    Variables = variables
                };

                var response = await _client!.SendMutationAsync<JsonElement>(request, cancellationToken);

                // Check for GraphQL errors
                if (response.Errors?.Length > 0)
                {
                    throw new InvalidOperationException(
                        $"GraphQL mutation failed: {JsonSerializer.Serialize(response.Errors)}");
                }

                return; // Success
            }
            catch (InvalidOperationException) when (attempt < _maxRetryCount)
            {
                attempt++;
                var backoff = _retryDelayMs * (int)Math.Pow(2, attempt - 1);
                await Task.Delay(backoff, cancellationToken);
            }
            catch (HttpRequestException) when (attempt < _maxRetryCount)
            {
                attempt++;
                var backoff = _retryDelayMs * (int)Math.Pow(2, attempt - 1);
                await Task.Delay(backoff, cancellationToken);
            }
        }
    }
}
