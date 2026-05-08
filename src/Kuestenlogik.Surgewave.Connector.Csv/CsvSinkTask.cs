using System.Globalization;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Csv;

/// <summary>
/// Task that writes records to CSV files.
/// Supports RFC 4180 compliant output with append, overwrite, and rolling modes.
/// </summary>
public sealed class CsvSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputPath = "";
    private string _delimiter = CsvConnectorConfig.DefaultDelimiter;
    private bool _includeHeader = CsvConnectorConfig.DefaultIncludeHeader;
    private System.Text.Encoding _encoding = System.Text.Encoding.UTF8;
    private string _outputMode = CsvConnectorConfig.DefaultOutputMode;
    private int _maxRecordsPerFile = CsvConnectorConfig.DefaultMaxRecordsPerFile;
    private string _fileNamePattern = CsvConnectorConfig.DefaultFileNamePattern;

    private StreamWriter? _writer;
    private CsvWriter? _csvWriter;
    private string _currentFilePath = "";
    private int _currentRecordCount;
    private bool _headerWritten;
    private string[]? _lastHeaders;

    private readonly List<SinkRecord> _buffer = [];

    public override void Start(IDictionary<string, string> config)
    {
        _outputPath = config.TryGetValue(CsvConnectorConfig.OutputPath, out var op) ? op : "";
        _delimiter = config.TryGetValue(CsvConnectorConfig.Delimiter, out var delim) ? delim : CsvConnectorConfig.DefaultDelimiter;

        if (config.TryGetValue(CsvConnectorConfig.IncludeHeader, out var includeHeader))
            _includeHeader = bool.Parse(includeHeader);

        var encodingName = config.TryGetValue(CsvConnectorConfig.Encoding, out var enc) ? enc : CsvConnectorConfig.DefaultEncoding;
        _encoding = System.Text.Encoding.GetEncoding(encodingName);

        _outputMode = config.TryGetValue(CsvConnectorConfig.OutputMode, out var mode) ? mode : CsvConnectorConfig.DefaultOutputMode;

        if (config.TryGetValue(CsvConnectorConfig.MaxRecordsPerFile, out var maxRecords))
            _maxRecordsPerFile = int.Parse(maxRecords);

        _fileNamePattern = config.TryGetValue(CsvConnectorConfig.FileNamePattern, out var fnp) ? fnp : CsvConnectorConfig.DefaultFileNamePattern;

        // Ensure output directory exists
        var directory = Path.GetDirectoryName(_outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);
    }

    public override void Stop()
    {
        CloseWriter();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        _buffer.AddRange(records);
        return Task.CompletedTask;
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0)
            return;

        foreach (var record in _buffer)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Check if we need to rotate files
            if (_outputMode == CsvConnectorConfig.OutputModeRolling &&
                _maxRecordsPerFile > 0 &&
                _currentRecordCount >= _maxRecordsPerFile)
            {
                CloseWriter();
            }

            // Ensure writer is open
            if (_csvWriter == null)
            {
                OpenWriter(record.Topic, record.Partition);
            }

            await WriteRecordAsync(record);
        }

        await _csvWriter!.FlushAsync();
        await _writer!.FlushAsync();

        _buffer.Clear();
    }

    private void OpenWriter(string topic, int partition)
    {
        var filePath = GetFilePath(topic, partition);
        var append = _outputMode == CsvConnectorConfig.OutputModeAppend && File.Exists(filePath);

        // Check if we need to write header
        _headerWritten = append && File.Exists(filePath) && new FileInfo(filePath).Length > 0;

        var fileMode = append ? FileMode.Append : FileMode.Create;
        var fileStream = new FileStream(filePath, fileMode, FileAccess.Write, FileShare.Read);
        _writer = new StreamWriter(fileStream, _encoding);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _delimiter,
            HasHeaderRecord = false, // We manage headers manually
        };

        _csvWriter = new CsvWriter(_writer, csvConfig);
        _currentFilePath = filePath;
        _currentRecordCount = 0;
    }

    private string GetFilePath(string topic, int partition)
    {
        if (_outputMode == CsvConnectorConfig.OutputModeRolling || !Path.HasExtension(_outputPath))
        {
            // Use pattern for rolling or directory output
            var fileName = _fileNamePattern
                .Replace("${topic}", topic)
                .Replace("${partition}", partition.ToString())
                .Replace("${timestamp}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));

            // If outputPath is a directory
            if (Directory.Exists(_outputPath) || !Path.HasExtension(_outputPath))
            {
                if (!Directory.Exists(_outputPath))
                    Directory.CreateDirectory(_outputPath);
                return Path.Combine(_outputPath, fileName);
            }

            // Add timestamp to filename
            var dir = Path.GetDirectoryName(_outputPath) ?? "";
            return Path.Combine(dir, fileName);
        }

        return _outputPath;
    }

    private async Task WriteRecordAsync(SinkRecord record)
    {
        if (_csvWriter == null || record.Value == null)
            return;

        // Parse JSON value to dictionary
        Dictionary<string, object?>? data;
        try
        {
            var json = System.Text.Encoding.UTF8.GetString(record.Value);
            data = JsonSerializer.Deserialize<Dictionary<string, object?>>(json);
        }
        catch (JsonException)
        {
            // If not JSON, write as single value
            data = new Dictionary<string, object?>
            {
                ["value"] = System.Text.Encoding.UTF8.GetString(record.Value)
            };
        }

        if (data == null)
            return;

        var headers = data.Keys.ToArray();

        // Write header if needed
        if (_includeHeader && !_headerWritten)
        {
            foreach (var header in headers)
            {
                _csvWriter.WriteField(header);
            }
            await _csvWriter.NextRecordAsync();
            _headerWritten = true;
            _lastHeaders = headers;
        }

        // Write values in header order (or data order if no header)
        var orderedHeaders = _lastHeaders ?? headers;
        foreach (var header in orderedHeaders)
        {
            var value = data.TryGetValue(header, out var v) ? FormatValue(v) : "";
            _csvWriter.WriteField(value);
        }

        await _csvWriter.NextRecordAsync();
        _currentRecordCount++;
    }

    private static string FormatValue(object? value)
    {
        if (value == null)
            return "";

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString() ?? "",
                JsonValueKind.Number => element.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                JsonValueKind.Null => "",
                _ => element.GetRawText()
            };
        }

        return value.ToString() ?? "";
    }

    private void CloseWriter()
    {
        _csvWriter?.Dispose();
        _csvWriter = null;
        _writer?.Dispose();
        _writer = null;
        _currentFilePath = "";
        _currentRecordCount = 0;
        _headerWritten = false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            CloseWriter();
        }
        base.Dispose(disposing);
    }
}
