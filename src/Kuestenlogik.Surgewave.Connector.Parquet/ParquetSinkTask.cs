using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Parquet;

/// <summary>
/// Task that writes records to Parquet files.
/// Supports compression and row group configuration.
/// </summary>
public sealed class ParquetSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputPath = "";
    private string _outputMode = ParquetConnectorConfig.DefaultOutputMode;
    private int _maxRecordsPerFile = ParquetConnectorConfig.DefaultMaxRecordsPerFile;
    private string _fileNamePattern = ParquetConnectorConfig.DefaultFileNamePattern;
    private CompressionMethod _compressionMethod = CompressionMethod.Gzip;
    private int _rowGroupSize = ParquetConnectorConfig.DefaultRowGroupSize;

    private string _currentFilePath = "";
    private int _currentRecordCount;
    private ParquetSchema? _schema;
    private readonly List<Dictionary<string, object?>> _buffer = [];

    public override void Start(IDictionary<string, string> config)
    {
        _outputPath = config.TryGetValue(ParquetConnectorConfig.OutputPath, out var op) ? op : "";
        _outputMode = config.TryGetValue(ParquetConnectorConfig.OutputMode, out var mode) ? mode : ParquetConnectorConfig.DefaultOutputMode;

        if (config.TryGetValue(ParquetConnectorConfig.MaxRecordsPerFile, out var maxRecords))
            _maxRecordsPerFile = int.Parse(maxRecords);

        _fileNamePattern = config.TryGetValue(ParquetConnectorConfig.FileNamePattern, out var fnp) ? fnp : ParquetConnectorConfig.DefaultFileNamePattern;

        if (config.TryGetValue(ParquetConnectorConfig.CompressionCodec, out var codec))
            _compressionMethod = GetCompressionMethod(codec);

        if (config.TryGetValue(ParquetConnectorConfig.RowGroupSize, out var rgs))
            _rowGroupSize = int.Parse(rgs);

        // Ensure output directory exists
        var directory = Path.GetDirectoryName(_outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);
    }

    public override void Stop()
    {
        FlushBuffer().GetAwaiter().GetResult();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null)
                continue;

            try
            {
                var json = System.Text.Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, object?>>(json);
                if (data != null)
                {
                    _buffer.Add(data);
                }
            }
            catch (JsonException)
            {
                // If not JSON, store as single value
                _buffer.Add(new Dictionary<string, object?>
                {
                    ["value"] = System.Text.Encoding.UTF8.GetString(record.Value)
                });
            }
        }

        return Task.CompletedTask;
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBuffer();
    }

    private async Task FlushBuffer()
    {
        if (_buffer.Count == 0)
            return;

        // Check if we need to rotate files
        if (_outputMode == ParquetConnectorConfig.OutputModeRolling &&
            _maxRecordsPerFile > 0 &&
            _currentRecordCount >= _maxRecordsPerFile)
        {
            _currentFilePath = "";
            _currentRecordCount = 0;
        }

        // Get file path
        if (string.IsNullOrEmpty(_currentFilePath))
        {
            _currentFilePath = GetFilePath();
        }

        // Build schema from all buffered records if needed
        if (_schema == null && _buffer.Count > 0)
        {
            _schema = InferSchema(_buffer);
        }

        if (_schema == null)
        {
            _buffer.Clear();
            return;
        }

        // Write data to Parquet file
        await WriteParquetFileAsync();

        _currentRecordCount += _buffer.Count;
        _buffer.Clear();
    }

    private string GetFilePath()
    {
        if (_outputMode == ParquetConnectorConfig.OutputModeRolling || !Path.HasExtension(_outputPath))
        {
            var fileName = _fileNamePattern
                .Replace("${topic}", "data")
                .Replace("${partition}", "0")
                .Replace("${timestamp}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));

            if (Directory.Exists(_outputPath) || !Path.HasExtension(_outputPath))
            {
                if (!Directory.Exists(_outputPath))
                    Directory.CreateDirectory(_outputPath);
                return Path.Combine(_outputPath, fileName);
            }

            var dir = Path.GetDirectoryName(_outputPath) ?? "";
            return Path.Combine(dir, fileName);
        }

        return _outputPath;
    }

    private static ParquetSchema InferSchema(List<Dictionary<string, object?>> samples)
    {
        var fields = new List<DataField>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var sample in samples)
        {
            foreach (var key in sample.Keys)
            {
                if (seen.Add(key))
                {
                    // Always use string type for simplicity - JSON values are strings anyway
                    fields.Add(new DataField<string>(key));
                }
            }
        }

        return new ParquetSchema(fields);
    }

    private async Task WriteParquetFileAsync()
    {
        if (_schema == null || _buffer.Count == 0)
            return;

        var columns = new List<DataColumn>();
        var fields = _schema.DataFields;

        foreach (var field in fields)
        {
            var values = _buffer.Select(d => GetStringValue(d, field.Name)).ToArray();
            columns.Add(new DataColumn(field, values));
        }

        // Append new row groups in append mode and for subsequent flushes of the current
        // rolling file — recreating the file here would truncate all previously flushed rows.
        // Overwrite mode deliberately replaces the file. A failure to open or append to an
        // existing file propagates to the worker (retry/DLQ) instead of discarding rows.
        var existingFile = new FileInfo(_currentFilePath);
        var append = existingFile.Exists && existingFile.Length > 0 &&
            (_outputMode == ParquetConnectorConfig.OutputModeAppend ||
             _outputMode == ParquetConnectorConfig.OutputModeRolling);

        // Write to file
        using var stream = append
            ? File.Open(_currentFilePath, FileMode.Open, FileAccess.ReadWrite)
            : File.Create(_currentFilePath);
        using var writer = await ParquetWriter.CreateAsync(_schema, stream, append: append);

        writer.CompressionMethod = _compressionMethod;

        // Write in row groups
        var rowCount = columns.Count > 0 ? columns[0].Data.Length : 0;
        var rowsWritten = 0;

        while (rowsWritten < rowCount)
        {
            var rowsToWrite = Math.Min(_rowGroupSize, rowCount - rowsWritten);

            using var groupWriter = writer.CreateRowGroup();

            foreach (var column in columns)
            {
                var slicedData = SliceArray(column.Data, rowsWritten, rowsToWrite);
                await groupWriter.WriteColumnAsync(new DataColumn(column.Field, slicedData));
            }

            rowsWritten += rowsToWrite;
        }
    }

    private static string GetStringValue(Dictionary<string, object?> data, string fieldName)
    {
        if (!data.TryGetValue(fieldName, out var value) || value == null)
            return "";

        return value switch
        {
            string s => s,
            JsonElement e => e.ValueKind == JsonValueKind.String ? e.GetString() ?? "" : e.GetRawText(),
            _ => value.ToString() ?? ""
        };
    }

    private static Array SliceArray(Array source, int offset, int length)
    {
        var elementType = source.GetType().GetElementType()!;
        var result = Array.CreateInstance(elementType, length);
        Array.Copy(source, offset, result, 0, length);
        return result;
    }

    private static CompressionMethod GetCompressionMethod(string codec)
    {
        return codec.ToLowerInvariant() switch
        {
            ParquetConnectorConfig.CompressionNone => CompressionMethod.None,
            ParquetConnectorConfig.CompressionGzip => CompressionMethod.Gzip,
            ParquetConnectorConfig.CompressionSnappy => CompressionMethod.Snappy,
            ParquetConnectorConfig.CompressionLz4 => CompressionMethod.LZ4,
            ParquetConnectorConfig.CompressionZstd => CompressionMethod.Zstd,
            ParquetConnectorConfig.CompressionBrotli => CompressionMethod.Brotli,
            _ => CompressionMethod.Gzip
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try
            {
                FlushBuffer().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                // Dispose must not throw — Stop() has already surfaced flush failures
                Context?.RaiseError?.Invoke(ex);
            }
        }
        base.Dispose(disposing);
    }
}
