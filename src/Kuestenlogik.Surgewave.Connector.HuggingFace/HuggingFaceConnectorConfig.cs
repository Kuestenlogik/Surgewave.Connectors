namespace Kuestenlogik.Surgewave.Connector.HuggingFace;

/// <summary>
/// Configuration constants for the Hugging Face connector.
/// </summary>
public static class HuggingFaceConnectorConfig
{
    // Connection configuration
    public const string TopicsConfig = "topics";
    public const string ApiKeyConfig = "huggingface.api.key";
    public const string ModelIdConfig = "huggingface.model.id";
    public const string EndpointConfig = "huggingface.endpoint";
    public const string DefaultEndpoint = "https://api-inference.huggingface.co/models";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeSentiment = "sentiment";
    public const string ModeNer = "ner";
    public const string ModeClassification = "classification";
    public const string ModeEmbeddings = "embeddings";
    public const string ModeTextGeneration = "text-generation";
    public const string ModeFillMask = "fill-mask";
    public const string ModeQuestionAnswering = "question-answering";
    public const string ModeSummarization = "summarization";
    public const string ModeTranslation = "translation";

    // Default models by mode
    public const string DefaultSentimentModel = "distilbert-base-uncased-finetuned-sst-2-english";
    public const string DefaultNerModel = "dslim/bert-base-NER";
    public const string DefaultClassificationModel = "facebook/bart-large-mnli";
    public const string DefaultEmbeddingsModel = "sentence-transformers/all-MiniLM-L6-v2";
    public const string DefaultTextGenerationModel = "gpt2";
    public const string DefaultFillMaskModel = "bert-base-uncased";
    public const string DefaultQuestionAnsweringModel = "deepset/roberta-base-squad2";
    public const string DefaultSummarizationModel = "facebook/bart-large-cnn";
    public const string DefaultTranslationModel = "Helsinki-NLP/opus-mt-en-de";

    // Input/Output configuration
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "result";
    public const string EmbeddingsFieldConfig = "embeddings.field";
    public const string DefaultEmbeddingsField = "embedding";

    // Question-Answering specific
    public const string ContextFieldConfig = "context.field";
    public const string DefaultContextField = "context";
    public const string QuestionFieldConfig = "question.field";
    public const string DefaultQuestionField = "question";

    // Classification specific
    public const string CandidateLabelsConfig = "candidate.labels";
    public const string MultiLabelConfig = "multi.label";
    public const bool DefaultMultiLabel = false;

    // Text generation specific
    public const string MaxNewTokensConfig = "max.new.tokens";
    public const int DefaultMaxNewTokens = 50;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 1.0;
    public const string TopKConfig = "top.k";
    public const int DefaultTopK = 50;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 0.95;
    public const string DoSampleConfig = "do.sample";
    public const bool DefaultDoSample = true;

    // Translation specific
    public const string SourceLanguageConfig = "source.language";
    public const string TargetLanguageConfig = "target.language";

    // Batching configuration
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 10;
    public const string BatchTimeoutMsConfig = "batch.timeout.ms";
    public const int DefaultBatchTimeoutMs = 5000;

    // Retry configuration
    public const string RetryMaxConfig = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMsConfig = "retry.backoff.ms";
    public const int DefaultRetryBackoffMs = 1000;

    // Output format configuration
    public const string IncludeOriginalConfig = "include.original";
    public const bool DefaultIncludeOriginal = true;
    public const string OutputFormatConfig = "output.format";
    public const string FormatJson = "json";
    public const string FormatMerge = "merge";

    // Webhook configuration
    public const string WebhookUrlConfig = "webhook.url";

    // Wait for model configuration (Hugging Face may need to load models)
    public const string WaitForModelConfig = "wait.for.model";
    public const bool DefaultWaitForModel = true;
}
