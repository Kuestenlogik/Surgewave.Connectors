using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Generator;

/// <summary>
/// Source connector that generates test messages with configurable templates.
/// Useful for testing, development, and load generation scenarios.
/// </summary>
[ConnectorMetadata(
    Name = "Generator Source",
    Description = "Generates test messages with configurable templates. Useful for testing, development, and load generation.",
    Author = "KL Surgewave",
    Tags = "generator,test,mock,load,development",
    Icon = "Cog")]
public sealed class GeneratorSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(GeneratorSourceTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(GeneratorConnectorConfig.Topic, ConfigType.String, "", Importance.High,
            "Target topic for generated messages")
        .Define(GeneratorConnectorConfig.MessageCount, ConfigType.Long, GeneratorConnectorConfig.DefaultMessageCount, Importance.Medium,
            "Total number of messages to generate (0 = unlimited)")
        .Define(GeneratorConnectorConfig.IntervalMs, ConfigType.Long, GeneratorConnectorConfig.DefaultIntervalMs, Importance.Medium,
            "Interval between message batches in milliseconds")
        .Define(GeneratorConnectorConfig.BatchSize, ConfigType.Int, GeneratorConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of messages to generate per batch")
        .Define(GeneratorConnectorConfig.KeyTemplate, ConfigType.String, GeneratorConnectorConfig.DefaultKeyTemplate, Importance.Medium,
            "Template for message key (supports placeholders)")
        .Define(GeneratorConnectorConfig.ValueTemplate, ConfigType.String, GeneratorConnectorConfig.DefaultValueTemplate, Importance.Medium,
            "Template for message value (supports placeholders)")
        .Define(GeneratorConnectorConfig.MessageFormat, ConfigType.String, GeneratorConnectorConfig.DefaultMessageFormat, Importance.Low,
            "Message format: json, string, or bytes")
        .Define(GeneratorConnectorConfig.SequenceStart, ConfigType.Long, GeneratorConnectorConfig.DefaultSequenceStart, Importance.Low,
            "Starting value for sequence generator")
        .Define(GeneratorConnectorConfig.SequenceStep, ConfigType.Long, GeneratorConnectorConfig.DefaultSequenceStep, Importance.Low,
            "Step increment for sequence generator")
        .Define(GeneratorConnectorConfig.RandomSeed, ConfigType.Long, 0L, Importance.Low,
            "Random seed for reproducible generation (0 = random)")
        .Define(GeneratorConnectorConfig.RandomStringLength, ConfigType.Int, GeneratorConnectorConfig.DefaultRandomStringLength, Importance.Low,
            "Length of random strings")
        .Define(GeneratorConnectorConfig.RandomIntMin, ConfigType.Int, GeneratorConnectorConfig.DefaultRandomIntMin, Importance.Low,
            "Minimum value for random integers")
        .Define(GeneratorConnectorConfig.RandomIntMax, ConfigType.Int, GeneratorConnectorConfig.DefaultRandomIntMax, Importance.Low,
            "Maximum value for random integers")
        .Define(GeneratorConnectorConfig.RandomDoubleMin, ConfigType.Double, GeneratorConnectorConfig.DefaultRandomDoubleMin, Importance.Low,
            "Minimum value for random doubles")
        .Define(GeneratorConnectorConfig.RandomDoubleMax, ConfigType.Double, GeneratorConnectorConfig.DefaultRandomDoubleMax, Importance.Low,
            "Maximum value for random doubles");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(GeneratorConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {GeneratorConnectorConfig.Topic}");

        // Validate message format
        var format = config.TryGetValue(GeneratorConnectorConfig.MessageFormat, out var f) ? f : GeneratorConnectorConfig.DefaultMessageFormat;
        if (format is not (GeneratorConnectorConfig.FormatJson or GeneratorConnectorConfig.FormatString or GeneratorConnectorConfig.FormatBytes))
            throw new ArgumentException($"Invalid message format: {format}. Must be 'json', 'string', or 'bytes'");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for consistent sequence generation
        return [new Dictionary<string, string>(_config)];
    }
}
