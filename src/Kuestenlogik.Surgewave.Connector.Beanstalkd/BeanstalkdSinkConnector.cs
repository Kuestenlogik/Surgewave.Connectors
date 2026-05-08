using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Sink connector that puts jobs into Beanstalkd tubes.
/// </summary>
[ConnectorMetadata(
    Name = "beanstalkd-sink",
    Description = "Puts jobs into Beanstalkd tubes",
    Author = "Surgewave",
    Tags = "beanstalkd, job, queue, messaging, sink")]
public sealed class BeanstalkdSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(BeanstalkdConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(BeanstalkdConnectorConfig.Host, ConfigType.String,
            BeanstalkdConnectorConfig.DefaultHost, Importance.High,
            "Beanstalkd server host")
        .Define(BeanstalkdConnectorConfig.Port, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "Beanstalkd server port")
        .Define(BeanstalkdConnectorConfig.Tube, ConfigType.String,
            BeanstalkdConnectorConfig.DefaultTube, Importance.High,
            "Tube to put jobs into")
        .Define(BeanstalkdConnectorConfig.Priority, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultPriority.ToString(), Importance.Medium,
            "Job priority (lower is higher priority, 0-4294967295)")
        .Define(BeanstalkdConnectorConfig.DelaySeconds, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultDelaySeconds.ToString(), Importance.Medium,
            "Delay in seconds before job becomes ready")
        .Define(BeanstalkdConnectorConfig.TtrSeconds, ConfigType.Int,
            BeanstalkdConnectorConfig.DefaultTtrSeconds.ToString(), Importance.Medium,
            "Time-to-run in seconds (time allowed for job processing)");

    public override Type TaskClass => typeof(BeanstalkdSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(BeanstalkdConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{BeanstalkdConnectorConfig.Topics}' is required");
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
