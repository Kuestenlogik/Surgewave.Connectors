using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner.Tests;

/// <summary>
/// Exercises the write side without a Spanner connection: how JSON values are mapped onto
/// column values, and what happens to a batch that cannot be written.
/// </summary>
public class SpannerSinkTaskTests
{
    [Fact]
    public void ConvertJsonValue_WholeNumber_BecomesAnInt64()
    {
        Assert.Equal(42L, Assert.IsType<long>(SpannerSinkTask.ConvertJsonValue(Element("42"))));
    }

    [Fact]
    public void ConvertJsonValue_FractionalNumber_BecomesADouble()
    {
        Assert.Equal(1.5, Assert.IsType<double>(SpannerSinkTask.ConvertJsonValue(Element("1.5"))));
    }

    [Fact]
    public void ConvertJsonValue_StringsAndBooleans_KeepTheirClrType()
    {
        Assert.Equal("Ada", Assert.IsType<string>(SpannerSinkTask.ConvertJsonValue(Element("\"Ada\""))));
        Assert.True(Assert.IsType<bool>(SpannerSinkTask.ConvertJsonValue(Element("true"))));
        Assert.False(Assert.IsType<bool>(SpannerSinkTask.ConvertJsonValue(Element("false"))));
    }

    [Fact]
    public void ConvertJsonValue_Null_StaysNull()
    {
        Assert.Null(SpannerSinkTask.ConvertJsonValue(Element("null")));
    }

    [Fact]
    public void ConvertJsonValue_NestedObjectsAndArrays_AreStoredAsRawJson()
    {
        Assert.Equal("""["a","b"]""", Assert.IsType<string>(SpannerSinkTask.ConvertJsonValue(Element("""["a","b"]"""))));
        Assert.Equal("""{"n":1}""", Assert.IsType<string>(SpannerSinkTask.ConvertJsonValue(Element("""{"n":1}"""))));
    }

    [Fact]
    public async Task PutAsync_WithAnUnparseableRecord_RaisesAndRethrowsInsteadOfSkippingIt()
    {
        var errors = new List<Exception>();
        using var task = new SpannerSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        var thrown = await Assert.ThrowsAnyAsync<JsonException>(() =>
            task.PutAsync([Record("this is not json")], TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_WhenTheFlushFails_RaisesAndRethrowsInsteadOfDroppingTheBatch()
    {
        // Start was deliberately not called, so opening the transaction fails. What is pinned
        // is where that failure goes: the whole write used to be swallowed by an empty catch
        // while the worker committed the offsets of rows Spanner never saw.
        var errors = new List<Exception>();
        using var task = new SpannerSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        var thrown = await Assert.ThrowsAnyAsync<Exception>(() =>
            task.PutAsync([Record("""{"OrderId":"1","Total":9.5}""")], TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_WithNothingToWrite_NeverOpensAConnection()
    {
        var errors = new List<Exception>();
        using var task = new SpannerSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        await task.PutAsync([], TestContext.Current.CancellationToken);
        await task.PutAsync([RecordWithoutValue()], TestContext.Current.CancellationToken);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task PutAsync_WithARecordThatCarriesNoColumns_NeverOpensAConnection()
    {
        var errors = new List<Exception>();
        using var task = new SpannerSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfig(SinkConfig());

        await task.PutAsync([Record("""{"data":{}}""")], TestContext.Current.CancellationToken);

        Assert.Empty(errors);
    }

    private static JsonElement Element(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "spanner-writes",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "spanner-writes",
        Partition = 0,
        Offset = 0,
        Value = null!
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [SpannerConnectorConfig.ProjectId] = "demo-project",
        [SpannerConnectorConfig.InstanceId] = "demo-instance",
        [SpannerConnectorConfig.DatabaseId] = "demo-database",
        [SpannerConnectorConfig.Topics] = "spanner-writes",
        [SpannerConnectorConfig.TargetTable] = "Orders",
        [SpannerConnectorConfig.BatchSize] = "1"
    };
}
