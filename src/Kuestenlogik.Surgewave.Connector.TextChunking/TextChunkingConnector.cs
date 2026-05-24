using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// A transform connector that splits text into chunks using various strategies.
/// Useful for preparing text for embeddings and RAG pipelines.
/// </summary>
public sealed class TextChunkingConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TextChunkingTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TextChunkingConfig.Topics, ConfigType.String, Importance.High,
            "Input topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(TextChunkingConfig.OutputTopic, ConfigType.String, Importance.High,
            "Output topic to write chunks to", EditorHint.Topic)
        .Define(TextChunkingConfig.Strategy, ConfigType.String,
            TextChunkingConfig.DefaultStrategy, Importance.Medium,
            "Chunking strategy: fixed-size, sentence, paragraph, recursive")
        .Define(TextChunkingConfig.ChunkSize, ConfigType.Int,
            TextChunkingConfig.DefaultChunkSize, Importance.Medium,
            "Maximum chunk size (characters or words)")
        .Define(TextChunkingConfig.ChunkOverlap, ConfigType.Int,
            TextChunkingConfig.DefaultChunkOverlap, Importance.Medium,
            "Overlap between consecutive chunks")
        .Define(TextChunkingConfig.ChunkUnit, ConfigType.String,
            TextChunkingConfig.DefaultChunkUnit, Importance.Low,
            "Unit for chunk size: characters or words")
        .Define(TextChunkingConfig.SentenceDelimiters, ConfigType.String,
            TextChunkingConfig.DefaultSentenceDelimiters, Importance.Low,
            "Characters that mark sentence boundaries")
        .Define(TextChunkingConfig.ParagraphSeparator, ConfigType.String,
            TextChunkingConfig.DefaultParagraphSeparator, Importance.Low,
            "String that separates paragraphs")
        .Define(TextChunkingConfig.RecursiveSeparators, ConfigType.String,
            TextChunkingConfig.DefaultRecursiveSeparators, Importance.Low,
            "Pipe-separated list of separators for recursive strategy")
        .Define(TextChunkingConfig.IncludeMetadata, ConfigType.Boolean,
            TextChunkingConfig.DefaultIncludeMetadata, Importance.Low,
            "Include chunk metadata in headers")
        .Define(TextChunkingConfig.MetadataPrefix, ConfigType.String,
            TextChunkingConfig.DefaultMetadataPrefix, Importance.Low,
            "Prefix for metadata header names")
        .Define(TextChunkingConfig.TrimWhitespace, ConfigType.Boolean,
            TextChunkingConfig.DefaultTrimWhitespace, Importance.Low,
            "Trim whitespace from chunks")
        .Define(TextChunkingConfig.SkipEmpty, ConfigType.Boolean,
            TextChunkingConfig.DefaultSkipEmpty, Importance.Low,
            "Skip empty chunks")
        .Define(TextChunkingConfig.MinChunkSize, ConfigType.Int,
            TextChunkingConfig.DefaultMinChunkSize, Importance.Low,
            "Minimum chunk size to output");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(TextChunkingConfig.Topics))
            throw new ArgumentException($"Missing required config: {TextChunkingConfig.Topics}");
        if (!config.ContainsKey(TextChunkingConfig.OutputTopic))
            throw new ArgumentException($"Missing required config: {TextChunkingConfig.OutputTopic}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Text chunking can run multiple tasks for parallelism
        var numTasks = Math.Min(maxTasks, 1);
        var configs = new List<IDictionary<string, string>>();

        for (var i = 0; i < numTasks; i++)
        {
            configs.Add(new Dictionary<string, string>(_config));
        }

        return configs;
    }
}
