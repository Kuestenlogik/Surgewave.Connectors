namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Serialises every test class that opens a NetMQ socket. NetMQ keeps one process-wide context;
/// running those classes in parallel would have them tear it down under each other.
/// </summary>
[CollectionDefinition(Name)]
public class NetMqSocketCollection : ICollectionFixture<NetMqContextFixture>
{
    /// <summary>Shared by this definition and every <see cref="CollectionAttribute"/> that joins it.</summary>
    public const string Name = "NetMQ sockets";
}
