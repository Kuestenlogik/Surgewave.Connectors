namespace Kuestenlogik.Surgewave.Connector.Sequence;

/// <summary>
/// Configuration constants for the sequence connector.
/// </summary>
public static class SequenceConnectorConfig
{
    /// <summary>
    /// JSON array of child source configurations. Each element should contain
    /// "connector.class" and any connector-specific configuration.
    /// </summary>
    public const string SourcesConfig = "sources";

    /// <summary>
    /// Topic to produce records to.
    /// </summary>
    public const string TopicConfig = "topic";

    /// <summary>
    /// Whether to continue to the next source on error (default: false).
    /// If false, errors will stop the sequence.
    /// </summary>
    public const string ContinueOnErrorConfig = "continue.on.error";
    public const bool DefaultContinueOnError = false;

    /// <summary>
    /// Maximum number of empty polls before considering a source complete (default: 3).
    /// </summary>
    public const string EmptyPollsBeforeAdvanceConfig = "empty.polls.before.advance";
    public const int DefaultEmptyPollsBeforeAdvance = 3;

    /// <summary>
    /// Delay in milliseconds between polls when source returns empty (default: 100).
    /// </summary>
    public const string EmptyPollDelayMsConfig = "empty.poll.delay.ms";
    public const int DefaultEmptyPollDelayMs = 100;

    /// <summary>
    /// Whether to include source index in record headers (default: true).
    /// </summary>
    public const string IncludeSourceIndexConfig = "include.source.index";
    public const bool DefaultIncludeSourceIndex = true;

    /// <summary>
    /// Header name for source index (default: "sequence.source.index").
    /// </summary>
    public const string SourceIndexHeaderConfig = "source.index.header";
    public const string DefaultSourceIndexHeader = "sequence.source.index";

    /// <summary>
    /// Behavior when all sources complete: "stop" or "restart" (default: "stop").
    /// </summary>
    public const string CompletionBehaviorConfig = "completion.behavior";
    public const string CompletionBehaviorStop = "stop";
    public const string CompletionBehaviorRestart = "restart";
    public const string DefaultCompletionBehavior = CompletionBehaviorStop;
}
