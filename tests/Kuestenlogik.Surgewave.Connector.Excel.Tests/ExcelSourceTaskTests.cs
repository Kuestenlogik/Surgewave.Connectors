using System.Text.Json;
using ClosedXML.Excel;
using Kuestenlogik.Surgewave.Connector.Excel;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel.Tests;

public class ExcelSourceTaskTests : IDisposable
{
    private readonly string _tempDir;

    public ExcelSourceTaskTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"excel-source-tests-{Guid.NewGuid()}");
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
        using var task = new ExcelSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_WithEmptyFilePaths_ReturnsEmptyRecords()
    {
        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = "",
            [ExcelConnectorConfig.Topic] = "test-topic"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_WithHeaderRow_ReadsDataCorrectly()
    {
        var filePath = CreateTestExcelFile(withHeader: true);

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.HasHeader] = "true"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(3, records.Count); // 3 data rows

        var firstRecord = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(firstRecord);
        Assert.Equal("Alice", firstRecord["Name"]?.ToString());
    }

    [Fact]
    public async Task PollAsync_WithoutHeaderRow_UsesColumnNumbers()
    {
        var filePath = CreateTestExcelFileWithoutHeader();

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.HasHeader] = "false"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(3, records.Count);

        var firstRecord = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(firstRecord);
        Assert.Contains("Column1", firstRecord.Keys);
    }

    [Fact]
    public async Task PollAsync_WithKeyColumn_SetsRecordKey()
    {
        var filePath = CreateTestExcelFile(withHeader: true);

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.HasHeader] = "true",
            [ExcelConnectorConfig.KeyColumn] = "Id"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.NotNull(records[0].Key);
        Assert.Equal("1", System.Text.Encoding.UTF8.GetString(records[0].Key!));
    }

    [Fact]
    public async Task PollAsync_WithBatchSize_LimitsBatchSize()
    {
        var filePath = CreateTestExcelFile(withHeader: true);

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.HasHeader] = "true",
            [ExcelConnectorConfig.BatchSize] = "1"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
    }

    [Fact]
    public async Task PollAsync_SetsCorrectHeaders()
    {
        var filePath = CreateTestExcelFile(withHeader: true);

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.NotNull(records[0].Headers);
        Assert.Contains("excel.file", records[0].Headers!.Keys);
        Assert.Contains("excel.sheet", records[0].Headers!.Keys);
        Assert.Contains("excel.row", records[0].Headers!.Keys);
    }

    [Fact]
    public async Task PollAsync_WithSpecificSheet_ReadsCorrectSheet()
    {
        var filePath = CreateMultiSheetExcelFile();

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.SheetName] = "Sheet2",
            [ExcelConnectorConfig.HasHeader] = "true"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var firstRecord = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(firstRecord);
        Assert.Equal("Product1", firstRecord["Product"]?.ToString());
    }

    [Fact]
    public async Task PollAsync_WithCellRange_ReadsOnlySpecifiedRange()
    {
        var filePath = CreateTestExcelFile(withHeader: true);

        using var task = new ExcelSourceTask();
        var config = new Dictionary<string, string>
        {
            [ExcelConnectorConfig.FilePath] = filePath,
            [ExcelConnectorConfig.Topic] = "test-topic",
            [ExcelConnectorConfig.HasHeader] = "true",
            [ExcelConnectorConfig.StartColumn] = "1",
            [ExcelConnectorConfig.EndColumn] = "2"
        };
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var firstRecord = JsonSerializer.Deserialize<Dictionary<string, object?>>(records[0].Value!);
        Assert.NotNull(firstRecord);
        Assert.Equal(2, firstRecord.Count); // Only 2 columns
    }

    private string CreateTestExcelFile(bool withHeader)
    {
        var filePath = Path.Combine(_tempDir, $"test-{Guid.NewGuid()}.xlsx");

        using var workbook = new XLWorkbook();
        var worksheet = workbook.Worksheets.Add("Data");

        var row = 1;
        if (withHeader)
        {
            worksheet.Cell(row, 1).Value = "Id";
            worksheet.Cell(row, 2).Value = "Name";
            worksheet.Cell(row, 3).Value = "Age";
            row++;
        }

        worksheet.Cell(row, 1).Value = 1;
        worksheet.Cell(row, 2).Value = "Alice";
        worksheet.Cell(row, 3).Value = 30;
        row++;

        worksheet.Cell(row, 1).Value = 2;
        worksheet.Cell(row, 2).Value = "Bob";
        worksheet.Cell(row, 3).Value = 25;
        row++;

        worksheet.Cell(row, 1).Value = 3;
        worksheet.Cell(row, 2).Value = "Charlie";
        worksheet.Cell(row, 3).Value = 35;

        workbook.SaveAs(filePath);
        return filePath;
    }

    private string CreateTestExcelFileWithoutHeader()
    {
        var filePath = Path.Combine(_tempDir, $"test-noheader-{Guid.NewGuid()}.xlsx");

        using var workbook = new XLWorkbook();
        var worksheet = workbook.Worksheets.Add("Data");

        worksheet.Cell(1, 1).Value = "Alice";
        worksheet.Cell(1, 2).Value = 30;

        worksheet.Cell(2, 1).Value = "Bob";
        worksheet.Cell(2, 2).Value = 25;

        worksheet.Cell(3, 1).Value = "Charlie";
        worksheet.Cell(3, 2).Value = 35;

        workbook.SaveAs(filePath);
        return filePath;
    }

    private string CreateMultiSheetExcelFile()
    {
        var filePath = Path.Combine(_tempDir, $"test-multisheet-{Guid.NewGuid()}.xlsx");

        using var workbook = new XLWorkbook();

        // Sheet1 with users
        var sheet1 = workbook.Worksheets.Add("Sheet1");
        sheet1.Cell(1, 1).Value = "Name";
        sheet1.Cell(1, 2).Value = "Age";
        sheet1.Cell(2, 1).Value = "Alice";
        sheet1.Cell(2, 2).Value = 30;

        // Sheet2 with products
        var sheet2 = workbook.Worksheets.Add("Sheet2");
        sheet2.Cell(1, 1).Value = "Product";
        sheet2.Cell(1, 2).Value = "Price";
        sheet2.Cell(2, 1).Value = "Product1";
        sheet2.Cell(2, 2).Value = 99.99;
        sheet2.Cell(3, 1).Value = "Product2";
        sheet2.Cell(3, 2).Value = 149.99;

        workbook.SaveAs(filePath);
        return filePath;
    }
}
