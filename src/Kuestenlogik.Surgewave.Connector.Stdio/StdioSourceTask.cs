using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Stdio;

/// <summary>
/// Task that reads lines from stdin and produces them as records.
/// Supports line-by-line reading or JSON object parsing.
/// </summary>
public sealed class StdioSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _inputFormat = StdioConnectorConfig.DefaultInputFormat;
#pragma warning disable CA2213 // Disposable fields should be disposed - We don't own Console.In
    private TextReader _reader = Console.In;
#pragma warning restore CA2213
    private long _lineNumber;
    private bool _endOfStream;
    private readonly Dictionary<string, object> _sourcePartition = new() { ["source"] = "stdin" };

    // For testing - allows injecting a custom reader
    internal TextReader Reader
    {
        get => _reader;
        set => _reader = value;
    }

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[StdioConnectorConfig.Topic];
        _inputFormat = config.TryGetValue(StdioConnectorConfig.InputFormat, out var format)
            ? format
            : StdioConnectorConfig.DefaultInputFormat;

        // Try to restore offset
        var storedOffset = Context.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null && storedOffset.TryGetValue(StdioConnectorConfig.OffsetLineNumber, out var lineNum))
        {
            _lineNumber = Convert.ToInt64(lineNum);
        }
    }

    public override void Stop()
    {
        // Don't dispose Console.In
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_endOfStream)
        {
            // Once stdin is exhausted, wait indefinitely (or until cancelled)
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return [];
        }

        var records = new List<SourceRecord>();
        var batchSize = 100;

        for (var i = 0; i < batchSize; i++)
        {
            string? line;
            try
            {
                line = await _reader.ReadLineAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (line == null)
            {
                _endOfStream = true;
                break;
            }

            _lineNumber++;

            if (_inputFormat == StdioConnectorConfig.InputFormatJson)
            {
                // Try to parse as JSON, skip invalid lines
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    // Valid JSON, use as-is
                }
                catch (JsonException)
                {
                    // Skip invalid JSON lines
                    continue;
                }
            }

            var sourceOffset = new Dictionary<string, object>
            {
                [StdioConnectorConfig.OffsetLineNumber] = _lineNumber
            };

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Value = Encoding.UTF8.GetBytes(line),
                Timestamp = DateTimeOffset.UtcNow
            });
        }

        if (records.Count == 0 && !_endOfStream)
        {
            // No data available yet, wait briefly
            await Task.Delay(100, cancellationToken);
        }

        return records;
    }
}
