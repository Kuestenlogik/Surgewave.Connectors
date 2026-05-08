using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.SpaCy;

/// <summary>
/// Sink connector that processes text using spaCy NLP server.
/// </summary>
[ConnectorMetadata(
    Name = "spacy-sink",
    Description = "Processes text using spaCy NLP server for tokenization, NER, POS tagging, and more",
    Author = "Surgewave",
    Tags = "spacy, nlp, ner, tokenization, pos, lemma, ai, sink")]
public sealed class SpaCySinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(SpaCyConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume text from", EditorHint.Topic)
        .Define(SpaCyConnectorConfig.OutputTopic, ConfigType.String, Importance.High,
            "Surgewave topic to produce NLP results to", EditorHint.Topic)
        .Define(SpaCyConnectorConfig.ServerUrl, ConfigType.String,
            SpaCyConnectorConfig.DefaultServerUrl, Importance.High,
            "spaCy server URL (e.g., http://localhost:8080)")
        .Define(SpaCyConnectorConfig.Model, ConfigType.String,
            SpaCyConnectorConfig.DefaultModel, Importance.Medium,
            "spaCy model to use (e.g., en_core_web_sm, en_core_web_lg)")
        .Define(SpaCyConnectorConfig.Operations, ConfigType.List,
            SpaCyConnectorConfig.DefaultOperations, Importance.Medium,
            "NLP operations: tokenize, ner, pos, lemma, dep, similarity, sentiment")
        .Define(SpaCyConnectorConfig.TextField, ConfigType.String,
            SpaCyConnectorConfig.DefaultTextField, Importance.Medium,
            "JSON field containing text to process")
        .Define(SpaCyConnectorConfig.IncludeText, ConfigType.Boolean, "true", Importance.Low,
            "Include original text in output")
        .Define(SpaCyConnectorConfig.IncludeVectors, ConfigType.Boolean, "false", Importance.Low,
            "Include word vectors in output")
        .Define(SpaCyConnectorConfig.DisablePipeline, ConfigType.List, "", Importance.Low,
            "Pipeline components to disable for performance")
        .Define(SpaCyConnectorConfig.BatchSize, ConfigType.Int,
            SpaCyConnectorConfig.DefaultBatchSize.ToString(), Importance.Low,
            "Batch size for processing");

    public override Type TaskClass => typeof(SpaCySinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SpaCyConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{SpaCyConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(SpaCyConnectorConfig.OutputTopic, out var outputTopic) ||
            string.IsNullOrWhiteSpace(outputTopic))
        {
            throw new ArgumentException($"'{SpaCyConnectorConfig.OutputTopic}' is required");
        }

        if (!config.TryGetValue(SpaCyConnectorConfig.ServerUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{SpaCyConnectorConfig.ServerUrl}' is required");
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
