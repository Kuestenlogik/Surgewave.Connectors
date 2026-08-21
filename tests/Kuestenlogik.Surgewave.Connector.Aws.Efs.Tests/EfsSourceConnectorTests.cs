namespace Kuestenlogik.Surgewave.Connector.Aws.Efs.Tests;

/// <summary>
/// Configuration handling of <see cref="EfsSourceConnector"/>.
/// </summary>
public class EfsSourceConnectorTests
{
    [Fact]
    public void Start_RejectsAConfigurationWithoutATopic()
    {
        using var connector = new EfsSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [EfsConnectorConfig.RegionConfig] = "eu-central-1" }));

        Assert.Contains(EfsConnectorConfig.TopicConfig, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TaskConfigs_HandTheWholeConfigurationToASingleTask()
    {
        using var connector = new EfsSourceConnector();
        connector.Start(new Dictionary<string, string>
        {
            [EfsConnectorConfig.TopicConfig] = "efs-events",
            [EfsConnectorConfig.RegionConfig] = "eu-central-1"
        });

        // Describe calls are cluster-wide, so sharding them across tasks would only duplicate work.
        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("eu-central-1", taskConfig[EfsConnectorConfig.RegionConfig]);
        Assert.Equal(typeof(EfsSourceTask), connector.TaskClass);
    }
}
