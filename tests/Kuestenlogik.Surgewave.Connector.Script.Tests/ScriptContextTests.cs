using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connector.Script;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Script.Tests;

/// <summary>
/// Tests for ScriptContext and ScriptResult.
/// </summary>
public sealed class ScriptContextTests
{
    [Fact]
    public void ScriptContext_KeyString_ReturnsUtf8String()
    {
        var context = new ScriptContext
        {
            Key = Encoding.UTF8.GetBytes("test-key")
        };

        Assert.Equal("test-key", context.KeyString);
    }

    [Fact]
    public void ScriptContext_KeyString_ReturnsNullWhenKeyIsNull()
    {
        var context = new ScriptContext
        {
            Key = null
        };

        Assert.Null(context.KeyString);
    }

    [Fact]
    public void ScriptContext_ValueString_ReturnsUtf8String()
    {
        var context = new ScriptContext
        {
            Value = Encoding.UTF8.GetBytes("test-value")
        };

        Assert.Equal("test-value", context.ValueString);
    }

    [Fact]
    public void ScriptContext_ValueString_ReturnsNullWhenValueIsNull()
    {
        var context = new ScriptContext
        {
            Value = null
        };

        Assert.Null(context.ValueString);
    }

    [Fact]
    public void ScriptContext_ValueJson_ParsesJsonDocument()
    {
        var json = "{\"name\":\"test\",\"value\":42}";
        var context = new ScriptContext
        {
            Value = Encoding.UTF8.GetBytes(json)
        };

        using var doc = context.ValueJson;
        Assert.NotNull(doc);
        Assert.Equal("test", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal(42, doc.RootElement.GetProperty("value").GetInt32());
    }

    [Fact]
    public void ScriptContext_ValueJson_ReturnsNullWhenValueIsNull()
    {
        var context = new ScriptContext
        {
            Value = null
        };

        Assert.Null(context.ValueJson);
    }

    [Fact]
    public void ScriptContext_ValueAs_DeserializesType()
    {
        var obj = new TestData { Name = "test", Count = 123 };
        var json = JsonSerializer.SerializeToUtf8Bytes(obj);

        var context = new ScriptContext
        {
            Value = json
        };

        var result = context.ValueAs<TestData>();
        Assert.NotNull(result);
        Assert.Equal("test", result.Name);
        Assert.Equal(123, result.Count);
    }

    [Fact]
    public void ScriptContext_ValueAs_ReturnsNullWhenValueIsNull()
    {
        var context = new ScriptContext
        {
            Value = null
        };

        Assert.Null(context.ValueAs<TestData>());
    }

    [Fact]
    public void ScriptContext_GetHeader_ReturnsHeaderValue()
    {
        var context = new ScriptContext
        {
            Headers = new Dictionary<string, byte[]>
            {
                ["Content-Type"] = Encoding.UTF8.GetBytes("application/json"),
                ["X-Custom"] = Encoding.UTF8.GetBytes("custom-value")
            }
        };

        Assert.Equal("application/json", context.GetHeader("Content-Type"));
        Assert.Equal("custom-value", context.GetHeader("X-Custom"));
    }

    [Fact]
    public void ScriptContext_GetHeader_ReturnsNullForMissingHeader()
    {
        var context = new ScriptContext
        {
            Headers = new Dictionary<string, byte[]>()
        };

        Assert.Null(context.GetHeader("Missing-Header"));
    }

    [Fact]
    public void ScriptContext_Metadata_IsWritable()
    {
        var context = new ScriptContext();

        context.Metadata["key1"] = "value1";
        context.Metadata["key2"] = 42;

        Assert.Equal("value1", context.Metadata["key1"]);
        Assert.Equal(42, context.Metadata["key2"]);
    }

    [Fact]
    public void ScriptResult_Emit_AddsRecordWithBytes()
    {
        var result = new ScriptResult();

        result.Emit(
            Encoding.UTF8.GetBytes("key"),
            Encoding.UTF8.GetBytes("value"),
            "output-topic"
        );

        Assert.Single(result.Records);
        Assert.Equal("key", Encoding.UTF8.GetString(result.Records[0].Key!));
        Assert.Equal("value", Encoding.UTF8.GetString(result.Records[0].Value!));
        Assert.Equal("output-topic", result.Records[0].Topic);
    }

    [Fact]
    public void ScriptResult_Emit_AddsRecordWithStrings()
    {
        var result = new ScriptResult();

        result.Emit("key", "value", "output-topic");

        Assert.Single(result.Records);
        Assert.Equal("key", Encoding.UTF8.GetString(result.Records[0].Key!));
        Assert.Equal("value", Encoding.UTF8.GetString(result.Records[0].Value!));
    }

    [Fact]
    public void ScriptResult_EmitJson_SerializesObject()
    {
        var result = new ScriptResult();
        var data = new TestData { Name = "test", Count = 99 };

        result.EmitJson("key", data);

        Assert.Single(result.Records);
        var json = Encoding.UTF8.GetString(result.Records[0].Value!);
        Assert.Contains("\"Name\":\"test\"", json);
        Assert.Contains("\"Count\":99", json);
    }

    [Fact]
    public void ScriptResult_EmitWithSameKey_SetsUseInputKey()
    {
        var result = new ScriptResult();

        result.EmitWithSameKey(Encoding.UTF8.GetBytes("value"));

        Assert.Single(result.Records);
        Assert.True(result.Records[0].UseInputKey);
    }

    [Fact]
    public void ScriptResult_Skip_IndicatesRecordShouldBeSkipped()
    {
        var result = new ScriptResult { Skip = true };

        Assert.True(result.Skip);
        Assert.Empty(result.Records);
    }

    [Fact]
    public void ScriptResult_MultipleEmits_AccumulatesRecords()
    {
        var result = new ScriptResult();

        result.Emit("key1", "value1");
        result.Emit("key2", "value2");
        result.Emit("key3", "value3");

        Assert.Equal(3, result.Records.Count);
    }

    private sealed class TestData
    {
        public string Name { get; set; } = "";
        public int Count { get; set; }
    }
}
