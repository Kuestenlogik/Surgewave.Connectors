using System.Globalization;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Csv;

/// <summary>
/// Task that reads records from CSV files and produces source records.
/// Supports RFC 4180 compliant parsing with configurable options.
/// </summary>
public sealed class CsvSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string[] _filePaths = [];
    private string _topic = "";
    private string _keyField = "";
    private string _delimiter = CsvConnectorConfig.DefaultDelimiter;
    private bool _hasHeader = CsvConnectorConfig.DefaultHasHeader;
    private System.Text.Encoding _encoding = System.Text.Encoding.UTF8;
    private bool _trimFields = CsvConnectorConfig.DefaultTrimFields;
    private bool _ignoreBlankLines = CsvConnectorConfig.DefaultIgnoreBlankLines;
    private bool _deleteAfterRead = CsvConnectorConfig.DefaultDeleteAfterRead;
    private bool _moveAfterRead;
    private string _processedDirectory = "";
    private long _pollIntervalMs = CsvConnectorConfig.DefaultPollIntervalMs;

    private int _currentFileIndex;
    private StreamReader? _currentReader;
    private CsvReader? _csvReader;
    private string[]? _headers;
    private long _currentLineNumber;
    private string _currentFilePath = "";
    private bool _endOfFile;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        var filePathConfig = config.TryGetValue(CsvConnectorConfig.FilePath, out var fp) ? fp : "";
        _filePaths = filePathConfig.Split(';', StringSplitOptions.RemoveEmptyEntries);

        _topic = config.TryGetValue(CsvConnectorConfig.Topic, out var topic) ? topic : "csv-data";
        _keyField = config.TryGetValue(CsvConnectorConfig.KeyField, out var kf) ? kf : "";
        _delimiter = config.TryGetValue(CsvConnectorConfig.Delimiter, out var delim) ? delim : CsvConnectorConfig.DefaultDelimiter;

        if (config.TryGetValue(CsvConnectorConfig.HasHeader, out var hasHeader))
            _hasHeader = bool.Parse(hasHeader);

        var encodingName = config.TryGetValue(CsvConnectorConfig.Encoding, out var enc) ? enc : CsvConnectorConfig.DefaultEncoding;
        _encoding = System.Text.Encoding.GetEncoding(encodingName);

        if (config.TryGetValue(CsvConnectorConfig.TrimFields, out var trimFields))
            _trimFields = bool.Parse(trimFields);

        if (config.TryGetValue(CsvConnectorConfig.IgnoreBlankLines, out var ignoreBlank))
            _ignoreBlankLines = bool.Parse(ignoreBlank);

        if (config.TryGetValue(CsvConnectorConfig.DeleteAfterRead, out var deleteAfter))
            _deleteAfterRead = bool.Parse(deleteAfter);

        if (config.TryGetValue(CsvConnectorConfig.MoveAfterRead, out var moveAfter))
            _moveAfterRead = bool.Parse(moveAfter);

        _processedDirectory = config.TryGetValue(CsvConnectorConfig.ProcessedDirectory, out var pd) ? pd : "";

        if (config.TryGetValue(CsvConnectorConfig.PollIntervalMs, out var pollInterval))
            _pollIntervalMs = long.Parse(pollInterval);

        _sourcePartition["connector"] = "csv";
        _sourcePartition["files"] = string.Join(";", _filePaths);

        // Check for stored offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(CsvConnectorConfig.OffsetFilePath, out var offsetFile))
            {
                var fileIndex = Array.IndexOf(_filePaths, offsetFile?.ToString());
                if (fileIndex >= 0)
                {
                    _currentFileIndex = fileIndex;
                    if (storedOffset.TryGetValue(CsvConnectorConfig.OffsetLineNumber, out var lineNum))
                    {
                        _currentLineNumber = Convert.ToInt64(lineNum);
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
        if (_csvReader == null && !_endOfFile)
        {
            if (!OpenNextFile())
                return records;
        }

        if (_csvReader == null || _endOfFile)
        {
            await Task.Delay((int)_pollIntervalMs, cancellationToken);
            return records;
        }

        // Read batch of records
        const int batchSize = 100;
        var count = 0;

        while (count < batchSize && await _csvReader.ReadAsync())
        {
            _currentLineNumber++;
            cancellationToken.ThrowIfCancellationRequested();

            var record = CreateSourceRecord();
            if (record != null)
            {
                records.Add(record);
                count++;
            }
        }

        // Check if we've reached end of file
        if (count == 0)
        {
            HandleEndOfFile();

            // Try to open next file
            if (_currentFileIndex < _filePaths.Length)
            {
                if (OpenNextFile())
                {
                    // Read from new file
                    while (count < batchSize && await _csvReader!.ReadAsync())
                    {
                        _currentLineNumber++;
                        cancellationToken.ThrowIfCancellationRequested();

                        var record = CreateSourceRecord();
                        if (record != null)
                        {
                            records.Add(record);
                            count++;
                        }
                    }
                }
            }
            else
            {
                _endOfFile = true;
            }
        }

        return records;
    }

    private bool OpenNextFile()
    {
        if (_currentFileIndex >= _filePaths.Length)
            return false;

        CloseCurrentFile();

        _currentFilePath = _filePaths[_currentFileIndex];
        _currentFileIndex++;

        if (!File.Exists(_currentFilePath))
            return OpenNextFile(); // Skip missing files

        _currentReader = new StreamReader(_currentFilePath, _encoding);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _delimiter,
            HasHeaderRecord = _hasHeader,
            TrimOptions = _trimFields ? TrimOptions.Trim : TrimOptions.None,
            IgnoreBlankLines = _ignoreBlankLines,
        };

        _csvReader = new CsvReader(_currentReader, csvConfig);
        _currentLineNumber = 0;

        // Read header if present
        if (_hasHeader && _csvReader.Read())
        {
            _csvReader.ReadHeader();
            _headers = _csvReader.HeaderRecord;
            _currentLineNumber++;
        }

        // Skip to stored offset if resuming
        while (_currentLineNumber < GetStoredLineNumber())
        {
            if (!_csvReader.Read())
                break;
            _currentLineNumber++;
        }

        return true;
    }

    private long GetStoredLineNumber()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return 0;

        if (storedOffset.TryGetValue(CsvConnectorConfig.OffsetFilePath, out var offsetFile) &&
            offsetFile?.ToString() == _currentFilePath &&
            storedOffset.TryGetValue(CsvConnectorConfig.OffsetLineNumber, out var lineNum))
        {
            return Convert.ToInt64(lineNum);
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
        _csvReader?.Dispose();
        _csvReader = null;
        _currentReader?.Dispose();
        _currentReader = null;
        _currentFilePath = "";
    }

    private SourceRecord? CreateSourceRecord()
    {
        if (_csvReader == null)
            return null;

        Dictionary<string, object?> data;

        if (_headers != null && _headers.Length > 0)
        {
            data = new Dictionary<string, object?>();
            for (var i = 0; i < _headers.Length; i++)
            {
                var value = _csvReader.TryGetField<string>(i, out var field) ? field : null;
                data[_headers[i]] = value;
            }
        }
        else
        {
            // No header - use indices
            data = new Dictionary<string, object?>();
            var fieldIndex = 0;
            while (_csvReader.TryGetField<string>(fieldIndex, out var field))
            {
                data[$"field_{fieldIndex}"] = field;
                fieldIndex++;
            }
        }

        var jsonValue = JsonSerializer.SerializeToUtf8Bytes(data);

        byte[]? key = null;
        if (!string.IsNullOrEmpty(_keyField) && data.TryGetValue(_keyField, out var keyValue) && keyValue != null)
        {
            key = System.Text.Encoding.UTF8.GetBytes(keyValue.ToString() ?? "");
        }

        var sourceOffset = new Dictionary<string, object>
        {
            [CsvConnectorConfig.OffsetFilePath] = _currentFilePath,
            [CsvConnectorConfig.OffsetLineNumber] = _currentLineNumber,
            [CsvConnectorConfig.OffsetFileModified] = File.GetLastWriteTimeUtc(_currentFilePath).Ticks
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = key,
            Value = jsonValue,
            SourcePartition = _sourcePartition,
            SourceOffset = sourceOffset,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["csv.file"] = System.Text.Encoding.UTF8.GetBytes(_currentFilePath),
                ["csv.line"] = System.Text.Encoding.UTF8.GetBytes(_currentLineNumber.ToString())
            }
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
