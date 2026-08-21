using NetMQ;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Owns the process-wide NetMQ settings for the socket-backed tests: no lingering on close, and
/// a context teardown at the end so the test host does not sit on NetMQ's I/O threads.
/// </summary>
public sealed class NetMqContextFixture : IDisposable
{
    public NetMqContextFixture() => NetMQConfig.Linger = TimeSpan.Zero;

    public void Dispose() => NetMQConfig.Cleanup(block: false);
}
