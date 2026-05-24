using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Source connector that reserves jobs from Beanstalkd tubes.
/// </summary>
[ConnectorMetadata(
    Name = "beanstalkd-source",
    Description = "Reserves and consumes jobs from Beanstalkd tubes",
    Author = "Surgewave",
    Tags = "beanstalkd, job, queue, messaging, source")]
public sealed class BeanstalkdSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(BeanstalkdConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce messages to", EditorHint.Topic)
        .Define(BeanstalkdConnectorConfig.Host, ConfigType.String,
            BeanstalkdConnectorConfig.DefaultHost, Importance.High,
            "Beanstalkd server host")
        .Define(BeanstalkdConnectorConfig.Port, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "Beanstalkd server port")
        .Define(BeanstalkdConnectorConfig.Tube, ConfigType.String,
            BeanstalkdConnectorConfig.DefaultTube, Importance.High,
            "Tube to watch for jobs")
        .Define(BeanstalkdConnectorConfig.ReserveTimeoutSeconds, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultReserveTimeoutSeconds.ToString(), Importance.Medium,
            "Timeout in seconds for reserve operation")
        .Define(BeanstalkdConnectorConfig.BatchSize, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Maximum number of jobs to reserve per poll")
        .Define(BeanstalkdConnectorConfig.PollTimeoutMs, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultPollTimeoutMs.ToString(), Importance.Medium,
            "Poll timeout in milliseconds");

    public override Type TaskClass => typeof(BeanstalkdSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(BeanstalkdConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{BeanstalkdConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(BeanstalkdConnectorConfig.Tube, out var tube) ||
            string.IsNullOrWhiteSpace(tube))
        {
            throw new ArgumentException($"'{BeanstalkdConnectorConfig.Tube}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
