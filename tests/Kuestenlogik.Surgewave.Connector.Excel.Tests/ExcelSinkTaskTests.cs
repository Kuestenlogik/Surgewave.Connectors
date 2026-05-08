using System.Text.Json;
using ClosedXML.Excel;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Excel;

namespace Kuestenlogik.Surgewave.Connector.Excel.Tests;

public class ExcelSinkTaskTests : IDisposable
{
    private readonly string _tempDir;

    public ExcelSinkTaskTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"excel-sink-tests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new ExcelSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_DoesNothing()
    {
        var outputPath = Path.Combine(_tempDir, "empty.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite"
        };
        task.Start(config);

        await task.PutAsync([], CancellationToken.None);
        task.Stop();

        Assert.True(File.Exists(outputPath));
    }

    [Fact]
    public async Task PutAsync_OverwriteMode_WritesJsonRecords()
    {
        var outputPath = Path.Combine(_tempDir, "output.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite",
            [ExcelConnectorConfig.IncludeHeader] = "true"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Name = "Alice", Age = 30 }),
            CreateSinkRecord(new { Name = "Bob", Age = 25 })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        // Verify output file
        using var workbook = new XLWorkbook(outputPath);
        var worksheet = workbook.Worksheet(1);

        Assert.Equal("Name", worksheet.Cell(1, 1).GetString());
        Assert.Equal("Age", worksheet.Cell(1, 2).GetString());
        Assert.Equal("Alice", worksheet.Cell(2, 1).GetString());
        Assert.Equal("Bob", worksheet.Cell(3, 1).GetString());
    }

    [Fact]
    public async Task PutAsync_AppendMode_AppendsToExistingFile()
    {
        var outputPath = Path.Combine(_tempDir, "append.xlsx");

        // Create initial file
        using (var workbook = new XLWorkbook())
        {
            var worksheet = workbook.Worksheets.Add("Sheet1");
            worksheet.Cell(1, 1).Value = "Name";
            worksheet.Cell(1, 2).Value = "Age";
            worksheet.Cell(2, 1).Value = "Existing";
            worksheet.Cell(2, 2).Value = 99;
            workbook.SaveAs(outputPath);
        }

        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "append",
            [ExcelConnectorConfig.IncludeHeader] = "true"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Name = "Alice", Age = 30 })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        // Verify appended data
        using var verifyWorkbook = new XLWorkbook(outputPath);
        var verifySheet = verifyWorkbook.Worksheet(1);

        Assert.Equal("Existing", verifySheet.Cell(2, 1).GetString());
        Assert.Equal("Alice", verifySheet.Cell(3, 1).GetString());
    }

    [Fact]
    public async Task PutAsync_RollingMode_CreatesFileWithLimitedRows()
    {
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = _tempDir,
            [ExcelConnectorConfig.OutputMode] = "rolling",
            [ExcelConnectorConfig.MaxRowsPerFile] = "2",
            [ExcelConnectorConfig.IncludeHeader] = "false",
            [ExcelConnectorConfig.FileNamePattern] = "data-${topic}.xlsx"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Value = "Row1" }, "topic1"),
            CreateSinkRecord(new { Value = "Row2" }, "topic1")
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        // Should have created at least one file
        var files = Directory.GetFiles(_tempDir, "*.xlsx");
        Assert.NotEmpty(files);

        // Verify the file has the expected rows
        using var workbook = new XLWorkbook(files[0]);
        var worksheet = workbook.Worksheet(1);
        Assert.Equal("Row1", worksheet.Cell(1, 1).GetString());
        Assert.Equal("Row2", worksheet.Cell(2, 1).GetString());
    }

    [Fact]
    public async Task PutAsync_WithCustomSheetName_UsesConfiguredSheet()
    {
        var outputPath = Path.Combine(_tempDir, "customsheet.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite",
            [ExcelConnectorConfig.OutputSheetName] = "CustomData"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Value = "Test" })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        using var workbook = new XLWorkbook(outputPath);
        Assert.True(workbook.Worksheets.Contains("CustomData"));
    }

    [Fact]
    public async Task PutAsync_WithoutHeader_DoesNotWriteHeaderRow()
    {
        var outputPath = Path.Combine(_tempDir, "noheader.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite",
            [ExcelConnectorConfig.IncludeHeader] = "false"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Name = "Alice" })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        using var workbook = new XLWorkbook(outputPath);
        var worksheet = workbook.Worksheet(1);

        // First row should be data, not header
        Assert.Equal("Alice", worksheet.Cell(1, 1).GetString());
    }

    [Fact]
    public async Task PutAsync_WithNumericValues_PreservesTypes()
    {
        var outputPath = Path.Combine(_tempDir, "types.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite",
            [ExcelConnectorConfig.IncludeHeader] = "false"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { IntValue = 42, DoubleValue = 3.14, BoolValue = true })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        using var workbook = new XLWorkbook(outputPath);
        var worksheet = workbook.Worksheet(1);

        Assert.Equal(42.0, worksheet.Cell(1, 1).GetDouble());
        Assert.Equal(3.14, worksheet.Cell(1, 2).GetDouble(), 2);
        Assert.True(worksheet.Cell(1, 3).GetBoolean());
    }

    [Fact]
    public async Task FlushAsync_SavesCurrentProgress()
    {
        var outputPath = Path.Combine(_tempDir, "flush.xlsx");
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = outputPath,
            [ExcelConnectorConfig.OutputMode] = "overwrite"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Name = "Alice" })
        };

        await task.PutAsync(records, CancellationToken.None);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        // File should exist after flush
        Assert.True(File.Exists(outputPath));
    }

    [Fact]
    public async Task PutAsync_WithDirectory_CreatesOutputFile()
    {
        using var task = new ExcelSinkTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.OutputPath] = _tempDir,
            [ExcelConnectorConfig.OutputMode] = "overwrite"
        };
        task.Start(config);

        var records = new[]
        {
            CreateSinkRecord(new { Value = "Test" })
        };

        await task.PutAsync(records, CancellationToken.None);
        task.Stop();

        Assert.True(File.Exists(Path.Combine(_tempDir, "output.xlsx")));
    }

    private static SinkRecord CreateSinkRecord(object data, string topic = "test-topic")
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = 0,
            Offset = 0,
            Key = null,
            Value = JsonSerializer.SerializeToUtf8Bytes(data),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>()
        };
    }
}
