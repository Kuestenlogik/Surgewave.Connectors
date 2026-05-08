namespace Kuestenlogik.Surgewave.Connector.SpaCy;

/// <summary>
/// Configuration constants for spaCy connector.
/// </summary>
public static class SpaCyConnectorConfig
{
    // Server settings
    public const string ServerUrl = "spacy.server.url";
    public const string Model = "spacy.model";  // en_core_web_sm, en_core_web_lg, etc.

    // Sink settings
    public const string Topics = "topics";
    public const string OutputTopic = "spacy.output.topic";

    // Processing options
    public const string Operations = "spacy.operations";  // Comma-separated: tokenize, ner, pos, lemma, dep, similarity, sentiment
    public const string IncludeText = "spacy.include.text";
    public const string IncludeVectors = "spacy.include.vectors";
    public const string DisablePipeline = "spacy.disable";  // Comma-separated components to disable

    // Input settings
    public const string TextField = "spacy.text.field";  // JSON field containing text to process
    public const string BatchSize = "spacy.batch.size";

    // Defaults
    public const string DefaultServerUrl = "http://localhost:8080";
    public const string DefaultModel = "en_core_web_sm";
    public const string DefaultOperations = "tokenize,ner,pos";
    public const string DefaultTextField = "text";
    public const int DefaultBatchSize = 10;
}
