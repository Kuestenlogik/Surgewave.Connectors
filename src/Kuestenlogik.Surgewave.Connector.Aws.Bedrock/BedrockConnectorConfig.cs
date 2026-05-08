namespace Kuestenlogik.Surgewave.Connector.Aws.Bedrock;

/// <summary>
/// Configuration constants for the AWS Bedrock connector.
/// </summary>
public static class BedrockConnectorConfig
{
    // Connection configuration
    public const string TopicsConfig = "topics";
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key.id";
    public const string SecretKeyConfig = "aws.secret.access.key";
    public const string EndpointConfig = "aws.endpoint";

    // Model configuration
    public const string ModelIdConfig = "model.id";
    public const string DefaultModelId = "anthropic.claude-3-5-sonnet-20241022-v2:0";

    // Model IDs
    public const string ModelClaudeSonnet35 = "anthropic.claude-3-5-sonnet-20241022-v2:0";
    public const string ModelClaudeHaiku35 = "anthropic.claude-3-5-haiku-20241022-v1:0";
    public const string ModelClaudeOpus = "anthropic.claude-3-opus-20240229-v1:0";
    public const string ModelLlama370B = "meta.llama3-70b-instruct-v1:0";
    public const string ModelLlama38B = "meta.llama3-8b-instruct-v1:0";
    public const string ModelTitanText = "amazon.titan-text-express-v1";
    public const string ModelTitanEmbed = "amazon.titan-embed-text-v2:0";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeChat = "chat";
    public const string ModeEmbeddings = "embeddings";

    // Completion configuration
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 4096;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 0.7;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 0.9;

    // Input/Output configuration
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "response";
    public const string EmbeddingsFieldConfig = "embeddings.field";
    public const string DefaultEmbeddingsField = "embedding";

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
}
