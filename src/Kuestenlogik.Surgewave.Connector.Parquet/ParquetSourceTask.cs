using System.Text.Json;
using Parquet;
using Parquet.Data;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Parquet;

/// <summary>
/// Task that reads records from Parquet files and produces source records.
/// Supports batch reading with schema inference.
/// </summary>
public sealed class ParquetSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string[] _filePaths = [];
    private string _topic = "";
    private int _batchSize = ParquetConnectorConfig.DefaultBatchSize;
    private long _pollIntervalMs = ParquetConnectorConfig.DefaultPollIntervalMs;
    private bool _deleteAfterRead = ParquetConnectorConfig.DefaultDeleteAfterRead;
    private bool _moveAfterRead;
    private string _processedDirectory = "";

    private int _currentFileIndex;
    private ParquetReader? _reader;
    private string _currentFilePath = "";
    private long _currentRowIndex;
    private int _currentRowGroupIndex;
    private DataColumn[]? _currentColumns;
    private string[]? _columnNames;
    private int _currentRowInGroup;
    private int _rowsInCurrentGroup;
    private bool _endOfFile;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        var filePathConfig = config.TryGetValue(ParquetConnectorConfig.FilePath, out var fp) ? fp : "";
        _filePaths = filePathConfig.Split(';', StringSplitOptions.RemoveEmptyEntries);

        _topic = config.TryGetValue(ParquetConnectorConfig.Topic, out var topic) ? topic : "parquet-data";

        if (config.TryGetValue(ParquetConnectorConfig.BatchSize, out var batchSize))
            _batchSize = int.Parse(batchSize);

        if (config.TryGetValue(ParquetConnectorConfig.PollIntervalMs, out var pollInterval))
            _pollIntervalMs = long.Parse(pollInterval);

        if (config.TryGetValue(ParquetConnectorConfig.DeleteAfterRead, out var deleteAfter))
            _deleteAfterRead = bool.Parse(deleteAfter);

        if (config.TryGetValue(ParquetConnectorConfig.MoveAfterRead, out var moveAfter))
            _moveAfterRead = bool.Parse(moveAfter);

        _processedDirectory = config.TryGetValue(ParquetConnectorConfig.ProcessedDirectory, out var pd) ? pd : "";

        _sourcePartition["connector"] = "parquet";
        _sourcePartition["files"] = string.Join(";", _filePaths);

        // Check for stored offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(ParquetConnectorConfig.OffsetFilePath, out var offsetFile))
            {
                var fileIndex = Array.IndexOf(_filePaths, offsetFile?.ToString());
                if (fileIndex >= 0)
                {
                    _currentFileIndex = fileIndex;
                    if (storedOffset.TryGetValue(ParquetConnectorConfig.OffsetRowIndex, out var rowIdx))
                    {
                        _currentRowIndex = Convert.ToInt64(rowIdx);
                    }
                }
            }
        }
    }

    public override void Stop()
    {
        CloseCurrentFile();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (_filePaths.Length == 0)
            return records;

        // Open next file if needed
        if (_reader == null && !_endOfFile)
        {
            if (!await OpenNextFileAsync())
                return records;
        }

        if (_reader == null || _endOfFile)
        {
            await Task.Delay((int)_pollIntervalMs, cancellationToken);
            return records;
        }

        // Read batch of records
        var count = 0;
        while (count < _batchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!await ReadNextRowAsync())
            {
                // End of current file
                HandleEndOfFile();

                // Try to open next file
                if (_currentFileIndex < _filePaths.Length)
                {
                    if (!await OpenNextFileAsync())
                        break;
                }
                else
                {
                    _endOfFile = true;
                    break;
                }
            }

            var record = CreateSourceRecord();
            if (record != null)
            {
                records.Add(record);
                count++;
            }
        }

        return records;
    }

    private async Task<bool> OpenNextFileAsync()
    {
        if (_currentFileIndex >= _filePaths.Length)
            return false;

        CloseCurrentFile();

        _currentFilePath = _filePaths[_currentFileIndex];
        _currentFileIndex++;

        if (!File.Exists(_currentFilePath))
            return await OpenNextFileAsync(); // Skip missing files

        _reader = await ParquetReader.CreateAsync(_currentFilePath);
        _currentRowGroupIndex = 0;
        _currentRowIndex = 0;
        _currentRowInGroup = 0;
        _rowsInCurrentGroup = 0;
        _currentColumns = null;

        // Get column names from schema
        var schema = _reader.Schema;
        _columnNames = schema.Fields.Select(f => f.Name).ToArray();

        // Skip to stored offset if resuming
        var storedRowIndex = GetStoredRowIndex();
        while (_currentRowIndex < storedRowIndex)
        {
            if (!await ReadNextRowAsync())
                break;
        }

        return true;
    }

    private async Task<bool> ReadNextRowAsync()
    {
        if (_reader == null)
            return false;

        // Need to read next row group?
        if (_currentColumns == null || _currentRowInGroup >= _rowsInCurrentGroup)
        {
            if (_currentRowGroupIndex >= _reader.RowGroupCount)
                return false;

            using var groupReader = _reader.OpenRowGroupReader(_currentRowGroupIndex);
            var columns = new List<DataColumn>();
            foreach (var field in _reader.Schema.DataFields)
            {
                var column = await groupReader.ReadColumnAsync(field);
                columns.Add(column);
            }
            _currentColumns = columns.ToArray();
            _rowsInCurrentGroup = _currentColumns.Length > 0 ? _currentColumns[0].Data.Length : 0;
            _currentRowGroupIndex++;
            _currentRowInGroup = 0;
        }

        if (_currentRowInGroup >= _rowsInCurrentGroup)
            return false;

        _currentRowIndex++;
        _currentRowInGroup++;
        return true;
    }

    private long GetStoredRowIndex()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return 0;

        if (storedOffset.TryGetValue(ParquetConnectorConfig.OffsetFilePath, out var offsetFile) &&
            offsetFile?.ToString() == _currentFilePath &&
            storedOffset.TryGetValue(ParquetConnectorConfig.OffsetRowIndex, out var rowIdx))
        {
            return Convert.ToInt64(rowIdx);
        }

        return 0;
    }

    private void HandleEndOfFile()
    {
        if (string.IsNullOrEmpty(_currentFilePath))
            return;

        var filePath = _currentFilePath;
        CloseCurrentFile();

        if (_deleteAfterRead && File.Exists(filePath))
        {
            File.Delete(filePath);
        }
        else if (_moveAfterRead && !string.IsNullOrEmpty(_processedDirectory))
        {
            if (!Directory.Exists(_processedDirectory))
                Directory.CreateDirectory(_processedDirectory);

            var fileName = Path.GetFileName(filePath);
            var destPath = Path.Combine(_processedDirectory, fileName);
            File.Move(filePath, destPath, overwrite: true);
        }
    }

    private void CloseCurrentFile()
    {
        _reader?.Dispose();
        _reader = null;
        _currentFilePath = "";
        _currentColumns = null;
        _columnNames = null;
    }

    private SourceRecord? CreateSourceRecord()
    {
        if (_currentColumns == null || _columnNames == null)
            return null;

        var rowIndex = _currentRowInGroup - 1;
        var data = new Dictionary<string, object?>();

        for (var i = 0; i < _columnNames.Length && i < _currentColumns.Length; i++)
        {
            var column = _currentColumns[i];
            var value = GetColumnValue(column, rowIndex);
            data[_columnNames[i]] = value;
        }

        var jsonValue = JsonSerializer.SerializeToUtf8Bytes(data);

        var sourceOffset = new Dictionary<string, object>
        {
            [ParquetConnectorConfig.OffsetFilePath] = _currentFilePath,
            [ParquetConnectorConfig.OffsetRowIndex] = _currentRowIndex,
            [ParquetConnectorConfig.OffsetFileModified] = File.GetLastWriteTimeUtc(_currentFilePath).Ticks
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = null,
            Value = jsonValue,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["parquet.file"] = System.Text.Encoding.UTF8.GetBytes(_currentFilePath),
                ["parquet.row"] = System.Text.Encoding.UTF8.GetBytes(_currentRowIndex.ToString())
            }
        };
    }

    private static object? GetColumnValue(DataColumn column, int rowIndex)
    {
        if (rowIndex < 0 || rowIndex >= column.Data.Length)
            return null;

        var value = column.Data.GetValue(rowIndex);
        if (value == null)
            return null;

        // Convert Parquet types to JSON-compatible types
        return value switch
        {
            byte[] bytes => Convert.ToBase64String(bytes),
            DateTimeOffset dto => dto.ToString("O"),
            DateTime dt => dt.ToString("O"),
            TimeSpan ts => ts.ToString(),
            decimal d => d,
            _ => value
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            CloseCurrentFile();
        }
        base.Dispose(disposing);
    }
}
