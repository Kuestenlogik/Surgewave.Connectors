using System.Text.Json;
using ClosedXML.Excel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel;

/// <summary>
/// Task that writes records to Excel files.
/// Supports append, overwrite, and rolling output modes.
/// </summary>
public sealed class ExcelSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputPath = "";
    private string _outputMode = ExcelConnectorConfig.DefaultOutputMode;
    private string _sheetName = ExcelConnectorConfig.DefaultOutputSheetName;
    private bool _includeHeader = ExcelConnectorConfig.DefaultIncludeHeader;
    private int _maxRowsPerFile = ExcelConnectorConfig.DefaultMaxRowsPerFile;
    private string _fileNamePattern = ExcelConnectorConfig.DefaultFileNamePattern;

    private XLWorkbook? _workbook;
    private IXLWorksheet? _worksheet;
    private string _currentFilePath = "";
    private int _currentRow = 1;
    private int _rowsInCurrentFile;
    private string[]? _headers;
    private bool _headerWritten;
    private readonly object _lock = new();

    public override void Start(IDictionary<string, string> config)
    {
        _outputPath = config.TryGetValue(ExcelConnectorConfig.OutputPath, out var op) ? op : "";

        _outputMode = config.TryGetValue(ExcelConnectorConfig.OutputMode, out var om)
            ? om : ExcelConnectorConfig.DefaultOutputMode;

        _sheetName = config.TryGetValue(ExcelConnectorConfig.OutputSheetName, out var sn)
            ? sn : ExcelConnectorConfig.DefaultOutputSheetName;

        if (config.TryGetValue(ExcelConnectorConfig.IncludeHeader, out var ih))
            _includeHeader = bool.Parse(ih);

        if (config.TryGetValue(ExcelConnectorConfig.MaxRowsPerFile, out var mr))
            _maxRowsPerFile = int.Parse(mr);

        _fileNamePattern = config.TryGetValue(ExcelConnectorConfig.FileNamePattern, out var fp)
            ? fp : ExcelConnectorConfig.DefaultFileNamePattern;

        // Initialize based on output mode
        if (_outputMode == ExcelConnectorConfig.OutputModeOverwrite)
        {
            InitializeNewFile();
        }
        else if (_outputMode == ExcelConnectorConfig.OutputModeAppend)
        {
            InitializeAppendMode();
        }
        // Rolling mode creates files on demand
    }

    public override void Stop()
    {
        lock (_lock)
        {
            SaveAndClose();
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return Task.CompletedTask;

        lock (_lock)
        {
            foreach (var record in records)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Check if we need to roll to a new file
                if (_outputMode == ExcelConnectorConfig.OutputModeRolling &&
                    _maxRowsPerFile > 0 &&
                    _rowsInCurrentFile >= _maxRowsPerFile)
                {
                    SaveAndClose();
                    InitializeRollingFile(record.Topic, record.Partition);
                }

                // Ensure we have an open workbook
                if (_workbook == null || _worksheet == null)
                {
                    if (_outputMode == ExcelConnectorConfig.OutputModeRolling)
                    {
                        InitializeRollingFile(record.Topic, record.Partition);
                    }
                    else
                    {
                        InitializeNewFile();
                    }
                }

                WriteRecord(record);
            }
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_workbook != null && !string.IsNullOrEmpty(_currentFilePath))
            {
                _workbook.SaveAs(_currentFilePath);
            }
        }

        return Task.CompletedTask;
    }

    private void InitializeNewFile()
    {
        // Determine file path
        if (Directory.Exists(_outputPath))
        {
            _currentFilePath = Path.Combine(_outputPath, "output.xlsx");
        }
        else
        {
            _currentFilePath = _outputPath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase)
                ? _outputPath
                : _outputPath + ".xlsx";

            // Ensure directory exists
            var dir = Path.GetDirectoryName(_currentFilePath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);
        }

        // Delete existing file in overwrite mode
        if (_outputMode == ExcelConnectorConfig.OutputModeOverwrite && File.Exists(_currentFilePath))
        {
            File.Delete(_currentFilePath);
        }

        _workbook = new XLWorkbook();
        _worksheet = _workbook.Worksheets.Add(_sheetName);
        _currentRow = 1;
        _rowsInCurrentFile = 0;
        _headerWritten = false;
        _headers = null;
    }

    private void InitializeAppendMode()
    {
        // Determine file path
        if (Directory.Exists(_outputPath))
        {
            _currentFilePath = Path.Combine(_outputPath, "output.xlsx");
        }
        else
        {
            _currentFilePath = _outputPath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase)
                ? _outputPath
                : _outputPath + ".xlsx";
        }

        if (File.Exists(_currentFilePath))
        {
            // Open existing file
            _workbook = new XLWorkbook(_currentFilePath);

            if (_workbook.Worksheets.Contains(_sheetName))
            {
                _worksheet = _workbook.Worksheet(_sheetName);
                var lastRow = _worksheet.LastRowUsed();
                _currentRow = lastRow?.RowNumber() + 1 ?? 1;
                _rowsInCurrentFile = _currentRow - 1;
                _headerWritten = _currentRow > 1; // Assume header exists if data exists

                // Try to read existing headers
                if (_headerWritten && _worksheet.Row(1).CellsUsed().Any())
                {
                    var headerRow = _worksheet.Row(1);
                    var lastCol = headerRow.LastCellUsed()?.Address.ColumnNumber ?? 0;
                    _headers = new string[lastCol];
                    for (var col = 1; col <= lastCol; col++)
                    {
                        _headers[col - 1] = headerRow.Cell(col).GetString();
                    }
                }
            }
            else
            {
                _worksheet = _workbook.Worksheets.Add(_sheetName);
                _currentRow = 1;
                _rowsInCurrentFile = 0;
                _headerWritten = false;
            }
        }
        else
        {
            // Create new file
            var dir = Path.GetDirectoryName(_currentFilePath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            _workbook = new XLWorkbook();
            _worksheet = _workbook.Worksheets.Add(_sheetName);
            _currentRow = 1;
            _rowsInCurrentFile = 0;
            _headerWritten = false;
        }

        _headers = null;
    }

    private void InitializeRollingFile(string topic, int partition)
    {
        var fileName = _fileNamePattern
            .Replace("${topic}", topic)
            .Replace("${timestamp}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"))
            .Replace("${partition}", partition.ToString());

        if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            fileName += ".xlsx";

        var dir = Directory.Exists(_outputPath) ? _outputPath : Path.GetDirectoryName(_outputPath);
        if (string.IsNullOrEmpty(dir))
            dir = ".";

        if (!Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        _currentFilePath = Path.Combine(dir, fileName);

        _workbook = new XLWorkbook();
        _worksheet = _workbook.Worksheets.Add(_sheetName);
        _currentRow = 1;
        _rowsInCurrentFile = 0;
        _headerWritten = false;
        _headers = null;
    }

    private void WriteRecord(SinkRecord record)
    {
        if (_worksheet == null)
            return;

        // Parse record value as JSON
        Dictionary<string, object?>? data = null;
        if (record.Value != null && record.Value.Length > 0)
        {
            try
            {
                data = JsonSerializer.Deserialize<Dictionary<string, object?>>(record.Value);
            }
            catch
            {
                // If not JSON, write raw value in first column
                data = new Dictionary<string, object?> { ["Value"] = System.Text.Encoding.UTF8.GetString(record.Value) };
            }
        }

        if (data == null || data.Count == 0)
            return;

        // Initialize headers from first record if not already set
        if (_headers == null)
        {
            _headers = data.Keys.ToArray();
        }

        // Write header row if needed
        if (_includeHeader && !_headerWritten)
        {
            for (var col = 0; col < _headers.Length; col++)
            {
                _worksheet.Cell(_currentRow, col + 1).Value = _headers[col];
            }
            _currentRow++;
            _headerWritten = true;
        }

        // Write data row
        for (var col = 0; col < _headers.Length; col++)
        {
            var header = _headers[col];
            if (data.TryGetValue(header, out var value))
            {
                SetCellValue(_worksheet.Cell(_currentRow, col + 1), value);
            }
        }

        // Write any new columns not in original headers
        var newColumns = data.Keys.Except(_headers).ToList();
        if (newColumns.Count > 0)
        {
            var existingCount = _headers.Length;
            _headers = [.. _headers, .. newColumns];

            // Update header row if headers are included
            if (_includeHeader)
            {
                for (var i = 0; i < newColumns.Count; i++)
                {
                    _worksheet.Cell(1, existingCount + i + 1).Value = newColumns[i];
                }
            }

            // Write values for new columns
            for (var i = 0; i < newColumns.Count; i++)
            {
                if (data.TryGetValue(newColumns[i], out var value))
                {
                    SetCellValue(_worksheet.Cell(_currentRow, existingCount + i + 1), value);
                }
            }
        }

        _currentRow++;
        _rowsInCurrentFile++;
    }

    private static void SetCellValue(IXLCell cell, object? value)
    {
        if (value == null)
            return;

        switch (value)
        {
            case JsonElement element:
                SetCellFromJsonElement(cell, element);
                break;
            case bool b:
                cell.Value = b;
                break;
            case int i:
                cell.Value = i;
                break;
            case long l:
                cell.Value = l;
                break;
            case double d:
                cell.Value = d;
                break;
            case decimal dec:
                cell.Value = dec;
                break;
            case DateTime dt:
                cell.Value = dt;
                break;
            case DateTimeOffset dto:
                cell.Value = dto.DateTime;
                break;
            default:
                cell.Value = value.ToString();
                break;
        }
    }

    private static void SetCellFromJsonElement(IXLCell cell, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                var str = element.GetString();
                // Try to parse as DateTime
                if (DateTime.TryParse(str, out var dt))
                    cell.Value = dt;
                else
                    cell.Value = str;
                break;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l))
                    cell.Value = l;
                else
                    cell.Value = element.GetDouble();
                break;
            case JsonValueKind.True:
                cell.Value = true;
                break;
            case JsonValueKind.False:
                cell.Value = false;
                break;
            case JsonValueKind.Null:
                // Leave cell empty
                break;
            default:
                cell.Value = element.ToString();
                break;
        }
    }

    private void SaveAndClose()
    {
        if (_workbook != null)
        {
            if (!string.IsNullOrEmpty(_currentFilePath))
            {
                _workbook.SaveAs(_currentFilePath);
            }
            _workbook.Dispose();
            _workbook = null;
        }

        _worksheet = null;
        _currentFilePath = "";
        _currentRow = 1;
        _rowsInCurrentFile = 0;
        _headerWritten = false;
        _headers = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_lock)
            {
                SaveAndClose();
            }
        }
        base.Dispose(disposing);
    }
}
