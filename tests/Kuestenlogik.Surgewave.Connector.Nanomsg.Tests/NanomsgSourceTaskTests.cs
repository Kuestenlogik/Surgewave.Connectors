using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg.Tests;

/// <summary>
/// Covers the mapping from a received nanomsg frame to a <c>SourceRecord</c>. The socket itself is
/// a native handle, so the task is configured through the same code path <c>Start</c> uses and the
/// frame is handed in directly.
/// </summary>
public class NanomsgSourceTaskTests
{
    [Fact]
    public void CreateRecord_CarriesTheSocketTypeAndPayloadSize()
    {
        using var task = new NanomsgSourceTask();
        task.Configure(SourceConfig());

        var record = task.CreateRecord(Encoding.UTF8.GetBytes("hello"));

        Assert.Equal("nanomsg-events", record.Topic);
        Assert.Equal("hello", Encoding.UTF8.GetString(record.Value));
        Assert.Equal("SUB", Encoding.UTF8.GetString(record.Headers!["nanomsg.socket.type"]));
        Assert.Equal("5", Encoding.UTF8.GetString(record.Headers!["nanomsg.size"]));
        Assert.Equal("nanomsg", (string)record.SourcePartition["source"]);
        Assert.Equal("SUB", (string)record.SourcePartition["socket"]);
    }

    [Fact]
    public void CreateRecord_GivesEveryFrameItsOwnOffsetAndKey()
    {
        using var task = new NanomsgSourceTask();
        task.Configure(SourceConfig());

        var first = task.CreateRecord([1, 2, 3]);
        var second = task.CreateRecord([4]);

        // nanomsg carries no message id of its own, so the per-task counter is the only thing that
        // keeps two frames apart in offset storage.
        Assert.Equal(1L, (long)first.SourceOffset["message_id"]);
        Assert.Equal(2L, (long)second.SourceOffset["message_id"]);
        Assert.Equal("1", Encoding.UTF8.GetString(first.Key!));
        Assert.Equal("2", Encoding.UTF8.GetString(second.Key!));
        Assert.Equal("3", Encoding.UTF8.GetString(first.Headers!["nanomsg.size"]));
        Assert.Equal("1", Encoding.UTF8.GetString(second.Headers!["nanomsg.size"]));
    }

    [Theory]
    [InlineData("sub", "SUB")]
    [InlineData("Pull", "PULL")]
    public void Configure_NormalisesTheSocketType(string configured, string expected)
    {
        using var task = new NanomsgSourceTask();

        var config = SourceConfig();
        config[NanomsgConnectorConfig.SocketType] = configured;
        task.Configure(config);

        var record = task.CreateRecord(Encoding.UTF8.GetBytes("x"));

        Assert.Equal(expected, Encoding.UTF8.GetString(record.Headers!["nanomsg.socket.type"]));
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NanomsgConnectorConfig.Topic] = "nanomsg-events",
        [NanomsgConnectorConfig.Endpoints] = "tcp://127.0.0.1:5555",
        [NanomsgConnectorConfig.SocketType] = "SUB"
    };
}
