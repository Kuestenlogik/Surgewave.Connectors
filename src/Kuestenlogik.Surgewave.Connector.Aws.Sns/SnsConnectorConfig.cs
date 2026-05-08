namespace Kuestenlogik.Surgewave.Connector.Aws.Sns;

/// <summary>
/// Configuration constants for AWS SNS connector.
/// </summary>
internal static class SnsConnectorConfig
{
    // AWS connection configs
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key";
    public const string SecretKeyConfig = "aws.secret.key";
    public const string EndpointConfig = "aws.endpoint";

    // SNS configs
    public const string TopicArnConfig = "aws.sns.topic.arn";
    public const string TopicsConfig = "topics";
    public const string MessageGroupIdConfig = "aws.sns.message.group.id";
    public const string SubjectConfig = "aws.sns.subject";

    // Header mapping
    public const string HeaderPrefixConfig = "aws.sns.header.prefix";

    // Default values
    public const string DefaultRegion = "us-east-1";
    public const string DefaultHeaderPrefix = "sns.";
}
