using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Task that splits text into chunks using configured strategy.
/// </summary>
public sealed class TextChunkingTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputTopic = "";
    private ITextChunker _chunker = null!;
    private bool _includeMetadata;
    private string _metadataPrefix = "";
    private bool _skipEmpty;

    public override void Start(IDictionary<string, string> config)
    {
        _outputTopic = config[TextChunkingConfig.OutputTopic];

        // Parse configuration with defaults
        var strategy = GetConfigOrDefault(config, TextChunkingConfig.Strategy, TextChunkingConfig.DefaultStrategy);
        var chunkSize = int.Parse(GetConfigOrDefault(config, TextChunkingConfig.ChunkSize, TextChunkingConfig.DefaultChunkSize.ToString()));
        var overlap = int.Parse(GetConfigOrDefault(config, TextChunkingConfig.ChunkOverlap, TextChunkingConfig.DefaultChunkOverlap.ToString()));
        var chunkUnit = GetConfigOrDefault(config, TextChunkingConfig.ChunkUnit, TextChunkingConfig.DefaultChunkUnit);
        var sentenceDelimiters = GetConfigOrDefault(config, TextChunkingConfig.SentenceDelimiters, TextChunkingConfig.DefaultSentenceDelimiters);
        var paragraphSeparator = GetConfigOrDefault(config, TextChunkingConfig.ParagraphSeparator, TextChunkingConfig.DefaultParagraphSeparator);
        var recursiveSeparators = GetConfigOrDefault(config, TextChunkingConfig.RecursiveSeparators, TextChunkingConfig.DefaultRecursiveSeparators);
        var trimWhitespace = bool.Parse(GetConfigOrDefault(config, TextChunkingConfig.TrimWhitespace, TextChunkingConfig.DefaultTrimWhitespace.ToString()));
        var minChunkSize = int.Parse(GetConfigOrDefault(config, TextChunkingConfig.MinChunkSize, TextChunkingConfig.DefaultMinChunkSize.ToString()));

        _includeMetadata = bool.Parse(GetConfigOrDefault(config, TextChunkingConfig.IncludeMetadata, TextChunkingConfig.DefaultIncludeMetadata.ToString()));
        _metadataPrefix = GetConfigOrDefault(config, TextChunkingConfig.MetadataPrefix, TextChunkingConfig.DefaultMetadataPrefix);
        _skipEmpty = bool.Parse(GetConfigOrDefault(config, TextChunkingConfig.SkipEmpty, TextChunkingConfig.DefaultSkipEmpty.ToString()));

        // Create chunker based on strategy
        _chunker = strategy.ToLowerInvariant() switch
        {
            TextChunkingConfig.StrategyFixedSize => new FixedSizeChunker(
                chunkSize,
                overlap,
                chunkUnit == TextChunkingConfig.ChunkUnitWords,
                trimWhitespace,
                minChunkSize),

            TextChunkingConfig.StrategySentence => new SentenceChunker(
                chunkSize,
                overlap,
                sentenceDelimiters,
                trimWhitespace,
                minChunkSize),

            TextChunkingConfig.StrategyParagraph => new ParagraphChunker(
                chunkSize,
                overlap,
                UnescapeSeparator(paragraphSeparator),
                trimWhitespace,
                minChunkSize),

            TextChunkingConfig.StrategyRecursive => new RecursiveChunker(
                chunkSize,
                overlap,
                ParseSeparators(recursiveSeparators),
                trimWhitespace,
                minChunkSize),

            _ => throw new ArgumentException($"Unknown chunking strategy: {strategy}")
        };
    }

    public override void Stop()
    {
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || Context.Producer == null)
            return;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
            {
                if (!_skipEmpty)
                {
                    // Pass through empty records if not skipping
                    await Context.Producer.ProduceAsync(_outputTopic, record.Key, record.Value ?? [], cancellationToken);
                }
                continue;
            }

            var text = Encoding.UTF8.GetString(record.Value);
            var chunks = _chunker.Chunk(text);

            foreach (var chunk in chunks)
            {
                if (_skipEmpty && string.IsNullOrWhiteSpace(chunk.Text))
                    continue;

                var chunkBytes = Encoding.UTF8.GetBytes(chunk.Text);
                var headers = CreateHeaders(record, chunk);

                await Context.Producer.ProduceAsync(_outputTopic, record.Key, chunkBytes, headers, cancellationToken);
            }
        }
    }

    private Dictionary<string, byte[]> CreateHeaders(SinkRecord originalRecord, TextChunk chunk)
    {
        var headers = new Dictionary<string, byte[]>();

        // Copy original headers
        if (originalRecord.Headers != null)
        {
            foreach (var kvp in originalRecord.Headers)
            {
                headers[kvp.Key] = kvp.Value;
            }
        }

        // Add chunk metadata if enabled
        if (_includeMetadata)
        {
            headers[$"{_metadataPrefix}index"] = Encoding.UTF8.GetBytes(chunk.Index.ToString());
            headers[$"{_metadataPrefix}total"] = Encoding.UTF8.GetBytes(chunk.TotalChunks.ToString());
            headers[$"{_metadataPrefix}start_offset"] = Encoding.UTF8.GetBytes(chunk.StartOffset.ToString());
            headers[$"{_metadataPrefix}end_offset"] = Encoding.UTF8.GetBytes(chunk.EndOffset.ToString());
            headers[$"{_metadataPrefix}original_topic"] = Encoding.UTF8.GetBytes(originalRecord.Topic);
            headers[$"{_metadataPrefix}original_partition"] = Encoding.UTF8.GetBytes(originalRecord.Partition.ToString());
            headers[$"{_metadataPrefix}original_offset"] = Encoding.UTF8.GetBytes(originalRecord.Offset.ToString());
        }

        return headers;
    }

    private static string GetConfigOrDefault(IDictionary<string, string> config, string key, string defaultValue)
    {
        return config.TryGetValue(key, out var value) ? value : defaultValue;
    }

    private static string UnescapeSeparator(string separator)
    {
        return separator
            .Replace("\\n", "\n")
            .Replace("\\r", "\r")
            .Replace("\\t", "\t");
    }

    private static string[] ParseSeparators(string separators)
    {
        // Separators are pipe-delimited
        return separators
            .Split('|')
            .Select(UnescapeSeparator)
            .ToArray();
    }
}
