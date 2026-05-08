using System.Text.Json;
using ClosedXML.Excel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel;

/// <summary>
/// Task that reads records from Excel files and produces source records.
/// Supports sheet selection, cell range mapping, and header detection.
/// </summary>
public sealed class ExcelSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string[] _filePaths = [];
    private string _topic = "";
    private string _sheetName = "";
    private int _sheetIndex = 1;
    private bool _hasHeader = ExcelConnectorConfig.DefaultHasHeader;
    private int _startRow = ExcelConnectorConfig.DefaultStartRow;
    private int _endRow;
    private int _startColumn = ExcelConnectorConfig.DefaultStartColumn;
    private int _endColumn;
    private string _keyColumn = "";
    private int _batchSize = ExcelConnectorConfig.DefaultBatchSize;
    private long _pollIntervalMs = ExcelConnectorConfig.DefaultPollIntervalMs;
    private bool _deleteAfterRead = ExcelConnectorConfig.DefaultDeleteAfterRead;
    private bool _moveAfterRead;
    private string _processedDirectory = "";

    private int _currentFileIndex;
    private XLWorkbook? _workbook;
    private IXLWorksheet? _worksheet;
    private string _currentFilePath = "";
    private int _currentRow;
    private int _lastRow;
    private int _lastColumn;
    private string[]? _headers;
    private bool _endOfFile;

    private readonly Dictionary<string, object> _sourcePartition = new();

    public override void Start(IDictionary<string, string> config)
    {
        var filePathConfig = config.TryGetValue(ExcelConnectorConfig.FilePath, out var fp) ? fp : "";
        _filePaths = filePathConfig.Split(';', StringSplitOptions.RemoveEmptyEntries);

        _topic = config.TryGetValue(ExcelConnectorConfig.Topic, out var topic) ? topic : "excel-data";
        _sheetName = config.TryGetValue(ExcelConnectorConfig.SheetName, out var sn) ? sn : "";

        if (config.TryGetValue(ExcelConnectorConfig.SheetIndex, out var si))
            _sheetIndex = int.Parse(si);

        if (config.TryGetValue(ExcelConnectorConfig.HasHeader, out var hh))
            _hasHeader = bool.Parse(hh);

        if (config.TryGetValue(ExcelConnectorConfig.StartRow, out var sr))
            _startRow = int.Parse(sr);

        if (config.TryGetValue(ExcelConnectorConfig.EndRow, out var er))
            _endRow = int.Parse(er);

        if (config.TryGetValue(ExcelConnectorConfig.StartColumn, out var sc))
            _startColumn = int.Parse(sc);

        if (config.TryGetValue(ExcelConnectorConfig.EndColumn, out var ec))
            _endColumn = int.Parse(ec);

        _keyColumn = config.TryGetValue(ExcelConnectorConfig.KeyColumn, out var kc) ? kc : "";

        if (config.TryGetValue(ExcelConnectorConfig.BatchSize, out var bs))
            _batchSize = int.Parse(bs);

        if (config.TryGetValue(ExcelConnectorConfig.PollIntervalMs, out var pi))
            _pollIntervalMs = long.Parse(pi);

        if (config.TryGetValue(ExcelConnectorConfig.DeleteAfterRead, out var dar))
            _deleteAfterRead = bool.Parse(dar);

        if (config.TryGetValue(ExcelConnectorConfig.MoveAfterRead, out var mar))
            _moveAfterRead = bool.Parse(mar);

        _processedDirectory = config.TryGetValue(ExcelConnectorConfig.ProcessedDirectory, out var pd) ? pd : "";

        _sourcePartition["connector"] = "excel";
        _sourcePartition["files"] = string.Join(";", _filePaths);

        // Check for stored offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null)
        {
            if (storedOffset.TryGetValue(ExcelConnectorConfig.OffsetFilePath, out var offsetFile))
            {
                var fileIndex = Array.IndexOf(_filePaths, offsetFile?.ToString());
                if (fileIndex >= 0)
                {
                    _currentFileIndex = fileIndex;
                    if (storedOffset.TryGetValue(ExcelConnectorConfig.OffsetRowIndex, out var rowIdx))
                    {
                        _currentRow = Convert.ToInt32(rowIdx);
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
        if (_worksheet == null && !_endOfFile)
        {
            if (!OpenNextFile())
                return records;
        }

        if (_worksheet == null || _endOfFile)
        {
            await Task.Delay((int)_pollIntervalMs, cancellationToken);
            return records;
        }

        // Read batch of records
        var count = 0;
        while (count < _batchSize && _currentRow <= _lastRow)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var record = CreateSourceRecord(_currentRow);
            _currentRow++;

            if (record != null)
            {
                records.Add(record);
                count++;
            }
        }

        // Check if we've reached end of sheet
        if (_currentRow > _lastRow)
        {
            HandleEndOfFile();

            // Try to open next file
            if (_currentFileIndex < _filePaths.Length)
            {
                if (OpenNextFile())
                {
                    // Read from new file
                    while (count < _batchSize && _currentRow <= _lastRow)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var record = CreateSourceRecord(_currentRow);
                        _currentRow++;

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

        try
        {
            _workbook = new XLWorkbook(_currentFilePath);

            // Get worksheet by name or index
            if (!string.IsNullOrEmpty(_sheetName))
            {
                _worksheet = _workbook.Worksheet(_sheetName);
            }
            else
            {
                _worksheet = _workbook.Worksheet(_sheetIndex);
            }

            // Determine data range
            var usedRange = _worksheet.RangeUsed();
            if (usedRange == null)
            {
                CloseCurrentFile();
                return OpenNextFile();
            }

            _lastRow = _endRow > 0 ? Math.Min(_endRow, usedRange.LastRow().RowNumber()) : usedRange.LastRow().RowNumber();
            _lastColumn = _endColumn > 0 ? Math.Min(_endColumn, usedRange.LastColumn().ColumnNumber()) : usedRange.LastColumn().ColumnNumber();

            // Read headers if present
            _currentRow = _startRow;
            if (_hasHeader)
            {
                _headers = new string[_lastColumn - _startColumn + 1];
                for (var col = _startColumn; col <= _lastColumn; col++)
                {
                    var cell = _worksheet.Cell(_currentRow, col);
                    _headers[col - _startColumn] = cell.GetString() ?? $"Column{col}";
                }
                _currentRow++;
            }
            else
            {
                // Generate column headers
                _headers = new string[_lastColumn - _startColumn + 1];
                for (var col = _startColumn; col <= _lastColumn; col++)
                {
                    _headers[col - _startColumn] = $"Column{col}";
                }
            }

            // Skip to stored offset if resuming
            var storedRow = GetStoredRowIndex();
            if (storedRow > _currentRow)
            {
                _currentRow = storedRow;
            }

            return true;
        }
        catch
        {
            CloseCurrentFile();
            return OpenNextFile();
        }
    }

    private int GetStoredRowIndex()
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset == null)
            return 0;

        if (storedOffset.TryGetValue(ExcelConnectorConfig.OffsetFilePath, out var offsetFile) &&
            offsetFile?.ToString() == _currentFilePath &&
            storedOffset.TryGetValue(ExcelConnectorConfig.OffsetRowIndex, out var rowIdx))
        {
            return Convert.ToInt32(rowIdx);
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
        _worksheet = null;
        _workbook?.Dispose();
        _workbook = null;
        _currentFilePath = "";
        _headers = null;
    }

    private SourceRecord? CreateSourceRecord(int rowNumber)
    {
        if (_worksheet == null || _headers == null)
            return null;

        var data = new Dictionary<string, object?>();

        for (var col = _startColumn; col <= _lastColumn; col++)
        {
            var cell = _worksheet.Cell(rowNumber, col);
            var headerIndex = col - _startColumn;
            var headerName = headerIndex < _headers.Length ? _headers[headerIndex] : $"Column{col}";

            data[headerName] = GetCellValue(cell);
        }

        var jsonValue = JsonSerializer.SerializeToUtf8Bytes(data);

        // Get key if specified
        byte[]? key = null;
        if (!string.IsNullOrEmpty(_keyColumn))
        {
            if (data.TryGetValue(_keyColumn, out var keyValue) && keyValue != null)
            {
                key = System.Text.Encoding.UTF8.GetBytes(keyValue.ToString() ?? "");
            }
            else if (int.TryParse(_keyColumn, out var keyColNum) && keyColNum >= _startColumn && keyColNum <= _lastColumn)
            {
                var cell = _worksheet.Cell(rowNumber, keyColNum);
                key = System.Text.Encoding.UTF8.GetBytes(GetCellValue(cell)?.ToString() ?? "");
            }
        }

        var sourceOffset = new Dictionary<string, object>
        {
            [ExcelConnectorConfig.OffsetFilePath] = _currentFilePath,
            [ExcelConnectorConfig.OffsetSheetName] = _worksheet.Name,
            [ExcelConnectorConfig.OffsetRowIndex] = rowNumber,
            [ExcelConnectorConfig.OffsetFileModified] = File.GetLastWriteTimeUtc(_currentFilePath).Ticks
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
                ["excel.file"] = System.Text.Encoding.UTF8.GetBytes(_currentFilePath),
                ["excel.sheet"] = System.Text.Encoding.UTF8.GetBytes(_worksheet.Name),
                ["excel.row"] = System.Text.Encoding.UTF8.GetBytes(rowNumber.ToString())
            }
        };
    }

    private static object? GetCellValue(IXLCell cell)
    {
        if (cell.IsEmpty())
            return null;

        return cell.DataType switch
        {
            XLDataType.Boolean => cell.GetBoolean(),
            XLDataType.Number => cell.GetDouble(),
            XLDataType.DateTime => cell.GetDateTime().ToString("O"),
            XLDataType.TimeSpan => cell.GetTimeSpan().ToString(),
            _ => cell.GetString()
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
