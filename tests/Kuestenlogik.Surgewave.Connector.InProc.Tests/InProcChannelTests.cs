using Kuestenlogik.Surgewave.Connector.InProc;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.InProc.Tests;

/// <summary>
/// Tests for the InProcChannel registry.
/// </summary>
public sealed class InProcChannelTests : IDisposable
{
    public InProcChannelTests()
    {
        // Clear all channels before each test
        InProcChannel.ClearAll();
    }

    public void Dispose()
    {
        InProcChannel.ClearAll();
    }

    [Fact]
    public void InProcChannel_GetOrCreate_CreatesChannel()
    {
        var channel = InProcChannel.GetOrCreate("test-channel");

        Assert.NotNull(channel);
        Assert.True(InProcChannel.Exists("test-channel"));
    }

    [Fact]
    public void InProcChannel_GetOrCreate_ReturnsSameChannel()
    {
        var channel1 = InProcChannel.GetOrCreate("same-channel");
        var channel2 = InProcChannel.GetOrCreate("same-channel");

        Assert.Same(channel1, channel2);
    }

    [Fact]
    public void InProcChannel_GetOrCreate_DifferentNamesReturnDifferentChannels()
    {
        var channel1 = InProcChannel.GetOrCreate("channel-1");
        var channel2 = InProcChannel.GetOrCreate("channel-2");

        Assert.NotSame(channel1, channel2);
    }

    [Fact]
    public void InProcChannel_Remove_RemovesChannel()
    {
        InProcChannel.GetOrCreate("to-remove");
        Assert.True(InProcChannel.Exists("to-remove"));

        var removed = InProcChannel.Remove("to-remove");

        Assert.True(removed);
        Assert.False(InProcChannel.Exists("to-remove"));
    }

    [Fact]
    public void InProcChannel_Remove_ReturnsFalseForNonexistent()
    {
        var removed = InProcChannel.Remove("nonexistent");
        Assert.False(removed);
    }

    [Fact]
    public void InProcChannel_Exists_ReturnsFalseForNonexistent()
    {
        Assert.False(InProcChannel.Exists("nonexistent"));
    }

    [Fact]
    public void InProcChannel_GetChannelNames_ReturnsAllNames()
    {
        InProcChannel.GetOrCreate("a");
        InProcChannel.GetOrCreate("b");
        InProcChannel.GetOrCreate("c");

        var names = InProcChannel.ChannelNames.ToList();

        Assert.Contains("a", names);
        Assert.Contains("b", names);
        Assert.Contains("c", names);
    }

    [Fact]
    public void InProcChannel_ClearAll_RemovesAllChannels()
    {
        InProcChannel.GetOrCreate("x");
        InProcChannel.GetOrCreate("y");

        InProcChannel.ClearAll();

        Assert.False(InProcChannel.Exists("x"));
        Assert.False(InProcChannel.Exists("y"));
        Assert.Empty(InProcChannel.ChannelNames);
    }

    [Fact]
    public async Task InProcChannel_CanWriteAndRead()
    {
        var channel = InProcChannel.GetOrCreate("readwrite");
        var message = new InProcMessage { Value = [1, 2, 3] };

        await channel.Writer.WriteAsync(message);
        var received = await channel.Reader.ReadAsync();

        Assert.Equal([1, 2, 3], received.Value);
    }

    [Fact]
    public async Task InProcChannel_PreservesMessageProperties()
    {
        var channel = InProcChannel.GetOrCreate("properties");
        var timestamp = DateTimeOffset.UtcNow;
        var headers = new Dictionary<string, byte[]> { ["header"] = [42] };

        var message = new InProcMessage
        {
            Key = [10],
            Value = [20],
            Headers = headers,
            Timestamp = timestamp
        };

        await channel.Writer.WriteAsync(message);
        var received = await channel.Reader.ReadAsync();

        Assert.Equal([10], received.Key);
        Assert.Equal([20], received.Value);
        Assert.Equal([42], received.Headers?["header"]);
        Assert.Equal(timestamp, received.Timestamp);
    }

    [Fact]
    public async Task InProcChannel_RespectsBufferSize()
    {
        var channel = InProcChannel.GetOrCreate("small-buffer", bufferSize: 2);

        // Write two messages (buffer full)
        await channel.Writer.WriteAsync(new InProcMessage { Value = [1] });
        await channel.Writer.WriteAsync(new InProcMessage { Value = [2] });

        // Third write should wait
        var writeTask = channel.Writer.WriteAsync(new InProcMessage { Value = [3] });
        await Task.Delay(50);
        Assert.False(writeTask.IsCompleted);

        // Read one to make space
        await channel.Reader.ReadAsync();

        // Now write should complete
        await writeTask.AsTask().WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void InProcMessage_HasDefaultValues()
    {
        var message = new InProcMessage();

        Assert.Null(message.Key);
        Assert.Empty(message.Value);
        Assert.Null(message.Headers);
        Assert.True(message.Timestamp > DateTimeOffset.MinValue);
    }
}
