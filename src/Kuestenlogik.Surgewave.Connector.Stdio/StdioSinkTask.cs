using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Stdio;

/// <summary>
/// Task that writes records to stdout or stderr.
/// Supports line-by-line output or JSON format with metadata.
/// </summary>
public sealed class StdioSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputFormat = StdioConnectorConfig.DefaultOutputFormat;
    private bool _includeKey = StdioConnectorConfig.DefaultIncludeKey;
    private bool _includeMetadata = StdioConnectorConfig.DefaultIncludeMetadata;
    private string _keyValueSeparator = StdioConnectorConfig.DefaultKeyValueSeparator;
#pragma warning disable CA2213 // Disposable fields should be disposed - We don't own Console.Out/Error
    private TextWriter _writer = Console.Out;
#pragma warning restore CA2213

    // For testing - allows injecting a custom writer
    internal TextWriter Writer
    {
        get => _writer;
        set => _writer = value;
    }

    public override void Start(IDictionary<string, string> config)
    {
        _outputFormat = config.TryGetValue(StdioConnectorConfig.OutputFormat, out var format)
            ? format
            : StdioConnectorConfig.DefaultOutputFormat;

        var outputTarget = config.TryGetValue(StdioConnectorConfig.OutputTarget, out var target)
            ? target
            : StdioConnectorConfig.DefaultOutputTarget;

        _writer = outputTarget == StdioConnectorConfig.OutputTargetStderr
            ? Console.Error
            : Console.Out;

        if (config.TryGetValue(StdioConnectorConfig.IncludeKey, out var includeKey))
            _includeKey = bool.Parse(includeKey);

        if (config.TryGetValue(StdioConnectorConfig.IncludeMetadata, out var includeMetadata))
            _includeMetadata = bool.Parse(includeMetadata);

        if (config.TryGetValue(StdioConnectorConfig.KeyValueSeparator, out var separator))
            _keyValueSeparator = separator;
    }

    public override void Stop()
    {
        // Don't dispose Console.Out or Console.Error
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var output = FormatRecord(record);
            await _writer.WriteLineAsync(output.AsMemory(), cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await _writer.FlushAsync(cancellationToken);
    }

    private string FormatRecord(SinkRecord record)
    {
        var value = record.Value != null ? Encoding.UTF8.GetString(record.Value) : "";
        var key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null;

        if (_outputFormat == StdioConnectorConfig.OutputFormatJson)
        {
            return FormatAsJson(record, key, value);
        }

        // Line format
        if (_includeKey && key != null)
        {
            return $"{key}{_keyValueSeparator}{value}";
        }

        return value;
    }

    private string FormatAsJson(SinkRecord record, string? key, string value)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();

        if (key != null)
        {
            writer.WriteString("key", key);
        }

        // Try to write value as JSON object if it's valid JSON
        try
        {
            using var doc = JsonDocument.Parse(value);
            writer.WritePropertyName("value");
            doc.RootElement.WriteTo(writer);
        }
        catch (JsonException)
        {
            // Not valid JSON, write as string
            writer.WriteString("value", value);
        }

        if (_includeMetadata)
        {
            writer.WriteString("topic", record.Topic);
            writer.WriteNumber("partition", record.Partition);
            writer.WriteNumber("offset", record.Offset);

            if (record.Timestamp != default)
            {
                writer.WriteString("timestamp", record.Timestamp.ToString("O"));
            }
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
