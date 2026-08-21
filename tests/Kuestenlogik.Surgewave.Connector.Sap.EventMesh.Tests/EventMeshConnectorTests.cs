using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh.Tests;

/// <summary>
/// Covers the validation both Event Mesh connectors perform before any task is handed out, plus
/// the shape of the task configuration itself.
/// </summary>
public class EventMeshConnectorTests
{
    [Theory]
    [InlineData(EventMeshConnectorConfig.Topic)]
    [InlineData(EventMeshConnectorConfig.ServiceUrl)]
    [InlineData(EventMeshConnectorConfig.QueueName)]
    public void SourceConnector_Start_WithoutARequiredKey_Throws(string missingKey)
    {
        using var connector = new EventMeshSourceConnector();
        var config = SourceConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_WithABlankQueueName_Throws()
    {
        // An empty queue name would build a consumption URL that addresses no queue at all.
        using var connector = new EventMeshSourceConnector();
        var config = SourceConfig();
        config[EventMeshConnectorConfig.QueueName] = "   ";

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(EventMeshConnectorConfig.QueueName, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(EventMeshConnectorConfig.Topics)]
    [InlineData(EventMeshConnectorConfig.ServiceUrl)]
    [InlineData(EventMeshConnectorConfig.TargetTopic)]
    public void SinkConnector_Start_WithoutARequiredKey_Throws(string missingKey)
    {
        using var connector = new EventMeshSinkConnector();
        var config = SinkConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TaskConfigs_HandOutAnIndependentCopyPerCall()
    {
        using var connector = new EventMeshSourceConnector();
        connector.Start(SourceConfig());

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[EventMeshConnectorConfig.QueueName] = "tampered";

        // A task that rewrites its own copy must not corrupt the connector's configuration.
        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("orders-queue", second[EventMeshConnectorConfig.QueueName]);
        Assert.Equal(typeof(EventMeshSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SourceConnector_Config_OffersOnlyTheImplementedAckModes()
    {
        using var connector = new EventMeshSourceConnector();

        var ackMode = Assert.Single(connector.Config.Keys, k => k.Name == EventMeshConnectorConfig.AckMode);

        Assert.Equal(new[] { "auto", "manual" }, ackMode.Options);
        // Manual acknowledgement is what keeps a message in the queue until CommitAsync ran.
        Assert.Equal(EventMeshConnectorConfig.DefaultAckMode, ackMode.DefaultValue);
    }

    [Fact]
    public void SinkConnector_Config_DeclaresTheCloudEventEnvelopeKeys()
    {
        using var connector = new EventMeshSinkConnector();
        var keys = connector.Config.Keys;

        Assert.Contains(keys, k => k.Name == EventMeshConnectorConfig.CloudEventSource && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == EventMeshConnectorConfig.CloudEventType && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == EventMeshConnectorConfig.ClientSecret && k.Type == ConfigType.Password);

        var contentType = Assert.Single(keys, k => k.Name == EventMeshConnectorConfig.ContentType);
        Assert.Equal(EventMeshConnectorConfig.DefaultContentType, contentType.DefaultValue);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [EventMeshConnectorConfig.Topic] = "eventmesh-events",
        [EventMeshConnectorConfig.ServiceUrl] = "https://em.test",
        [EventMeshConnectorConfig.TokenUrl] = "https://auth.test/oauth/token",
        [EventMeshConnectorConfig.ClientId] = "client-1",
        [EventMeshConnectorConfig.ClientSecret] = "secret-1",
        [EventMeshConnectorConfig.QueueName] = "orders-queue"
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [EventMeshConnectorConfig.Topics] = "orders",
        [EventMeshConnectorConfig.ServiceUrl] = "https://em.test",
        [EventMeshConnectorConfig.TokenUrl] = "https://auth.test/oauth/token",
        [EventMeshConnectorConfig.ClientId] = "client-1",
        [EventMeshConnectorConfig.ClientSecret] = "secret-1",
        [EventMeshConnectorConfig.TargetTopic] = "orders/created"
    };
}
