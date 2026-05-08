using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Manages named in-process channels for pub/sub communication.
/// Thread-safe singleton registry for named channels.
/// </summary>
public static class InProcChannel
{
    private static readonly ConcurrentDictionary<string, ChannelInstance> _channels = new();

    /// <summary>
    /// Gets or creates a named channel.
    /// </summary>
    public static Channel<InProcMessage> GetOrCreate(string name, int bufferSize = InProcConnectorConfig.DefaultBufferSize)
    {
        var instance = _channels.GetOrAdd(name, _ => new ChannelInstance(bufferSize));
        return instance.Channel;
    }

    /// <summary>
    /// Removes a named channel.
    /// </summary>
    public static bool Remove(string name)
    {
        return _channels.TryRemove(name, out _);
    }

    /// <summary>
    /// Checks if a channel exists.
    /// </summary>
    public static bool Exists(string name)
    {
        return _channels.ContainsKey(name);
    }

    /// <summary>
    /// Gets all channel names.
    /// </summary>
    public static IEnumerable<string> ChannelNames => _channels.Keys;

    /// <summary>
    /// Clears all channels. Used primarily for testing.
    /// </summary>
    public static void ClearAll()
    {
        _channels.Clear();
    }

    private sealed class ChannelInstance
    {
        public Channel<InProcMessage> Channel { get; }

        public ChannelInstance(int bufferSize)
        {
            Channel = System.Threading.Channels.Channel.CreateBounded<InProcMessage>(
                new BoundedChannelOptions(bufferSize)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });
        }
    }
}

/// <summary>
/// Message wrapper for in-process communication.
/// </summary>
public sealed record InProcMessage
{
    public byte[]? Key { get; init; }
    public byte[] Value { get; init; } = [];
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}
