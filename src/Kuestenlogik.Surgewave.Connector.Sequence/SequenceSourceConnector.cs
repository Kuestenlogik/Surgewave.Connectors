namespace Kuestenlogik.Surgewave.Connector.Sequence;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads from multiple child sources in sequence.
/// When one source completes, it advances to the next source in the list.
/// </summary>
public sealed class SequenceSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SequenceSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SequenceConnectorConfig.SourcesConfig, ConfigType.String, Importance.High,
            "JSON array of child source configurations")
        .Define(SequenceConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Topic to produce records to", EditorHint.Topic)
        .Define(SequenceConnectorConfig.ContinueOnErrorConfig, ConfigType.Boolean, SequenceConnectorConfig.DefaultContinueOnError, Importance.Medium,
            "Whether to continue to next source on error")
        .Define(SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig, ConfigType.Int, (long)SequenceConnectorConfig.DefaultEmptyPollsBeforeAdvance, Importance.Medium,
            "Number of empty polls before advancing to next source")
        .Define(SequenceConnectorConfig.EmptyPollDelayMsConfig, ConfigType.Int, (long)SequenceConnectorConfig.DefaultEmptyPollDelayMs, Importance.Low,
            "Delay between polls when source returns empty")
        .Define(SequenceConnectorConfig.IncludeSourceIndexConfig, ConfigType.Boolean, SequenceConnectorConfig.DefaultIncludeSourceIndex, Importance.Low,
            "Include source index in record headers")
        .Define(SequenceConnectorConfig.SourceIndexHeaderConfig, ConfigType.String, SequenceConnectorConfig.DefaultSourceIndexHeader, Importance.Low,
            "Header name for source index")
        .Define(SequenceConnectorConfig.CompletionBehaviorConfig, ConfigType.String, SequenceConnectorConfig.DefaultCompletionBehavior, Importance.Medium,
            "Behavior when all sources complete: 'stop' or 'restart'", EditorHint.Select, options: ["stop", "restart", "idle"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(SequenceConnectorConfig.SourcesConfig, out var sources) || string.IsNullOrWhiteSpace(sources))
            throw new ArgumentException($"Missing required config: {SequenceConnectorConfig.SourcesConfig}");

        if (!config.TryGetValue(SequenceConnectorConfig.TopicConfig, out var topic) || string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException($"Missing required config: {SequenceConnectorConfig.TopicConfig}");

        // Validate completion behavior
        if (config.TryGetValue(SequenceConnectorConfig.CompletionBehaviorConfig, out var behavior))
        {
            var validBehaviors = new[]
            {
                SequenceConnectorConfig.CompletionBehaviorStop,
                SequenceConnectorConfig.CompletionBehaviorRestart
            };

            if (!validBehaviors.Contains(behavior))
                throw new ArgumentException($"Invalid completion behavior: {behavior}. Must be one of: {string.Join(", ", validBehaviors)}");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Sequence connector runs as a single task to maintain ordering
        return [new Dictionary<string, string>(_config)];
    }
}
