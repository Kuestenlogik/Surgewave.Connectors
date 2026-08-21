using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests;

public class MirrorSourceConnectorTests
{
    private static Dictionary<string, string> BaseConfig() => new()
    {
        ["source.cluster.alias"] = "dc1",
        ["target.cluster.alias"] = "dc2",
        ["source.bootstrap.servers"] = "localhost:9092",
        ["target.bootstrap.servers"] = "remote:9092"
    };

    [Fact]
    public void Start_WithUnsupportedSecurityProtocol_ThrowsInsteadOfIgnoringIt()
    {
        using var connector = new MirrorSourceConnector();
        connector.Initialize(new ConnectorContext());

        var config = BaseConfig();
        config["source.security.protocol"] = "SASL_SSL";

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("source.security.protocol", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithSaslCredentials_ThrowsInsteadOfIgnoringThem()
    {
        using var connector = new MirrorSourceConnector();
        connector.Initialize(new ConnectorContext());

        var config = BaseConfig();
        config["target.sasl.mechanism"] = "PLAIN";

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("target.sasl.mechanism", exception.Message, StringComparison.Ordinal);
    }
}
