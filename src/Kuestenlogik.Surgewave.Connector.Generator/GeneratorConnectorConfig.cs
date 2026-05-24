namespace Kuestenlogik.Surgewave.Connector.Generator;

/// <summary>
/// Configuration constants for Generator connector.
/// Supports message generation with templates for testing and development.
/// </summary>
public static class GeneratorConnectorConfig
{
    // Required settings
    public const string Topic = "generator.topic";

    // Generation settings
    public const string MessageCount = "generator.message.count";
    public const string IntervalMs = "generator.interval.ms";
    public const string BatchSize = "generator.batch.size";

    // Template settings
    public const string KeyTemplate = "generator.key.template";
    public const string ValueTemplate = "generator.value.template";
    public const string MessageFormat = "generator.message.format";

    // Field generators
    public const string SequenceStart = "generator.sequence.start";
    public const string SequenceStep = "generator.sequence.step";

    // Random settings
    public const string RandomSeed = "generator.random.seed";
    public const string RandomStringLength = "generator.random.string.length";
    public const string RandomIntMin = "generator.random.int.min";
    public const string RandomIntMax = "generator.random.int.max";
    public const string RandomDoubleMin = "generator.random.double.min";
    public const string RandomDoubleMax = "generator.random.double.max";

    // Defaults
    public const long DefaultMessageCount = 0; // 0 = unlimited
    public const long DefaultIntervalMs = 1000;
    public const int DefaultBatchSize = 1;
    public const string DefaultKeyTemplate = "${sequence}";
    public const string DefaultValueTemplate = "{\"id\":${sequence},\"timestamp\":\"${timestamp}\",\"uuid\":\"${uuid}\",\"value\":${random_int}}";
    public const string DefaultMessageFormat = FormatJson;
    public const long DefaultSequenceStart = 1;
    public const long DefaultSequenceStep = 1;
    public const int DefaultRandomStringLength = 10;
    public const int DefaultRandomIntMin = 0;
    public const int DefaultRandomIntMax = 1000;
    public const double DefaultRandomDoubleMin = 0.0;
    public const double DefaultRandomDoubleMax = 1.0;

    // Message formats
    public const string FormatJson = "json";
    public const string FormatString = "string";
    public const string FormatBytes = "bytes";

    // Template placeholders
    public const string PlaceholderSequence = "${sequence}";
    public const string PlaceholderTimestamp = "${timestamp}";
    public const string PlaceholderTimestampMs = "${timestamp_ms}";
    public const string PlaceholderUuid = "${uuid}";
    public const string PlaceholderRandomInt = "${random_int}";
    public const string PlaceholderRandomDouble = "${random_double}";
    public const string PlaceholderRandomString = "${random_string}";
    public const string PlaceholderRandomBool = "${random_bool}";
    public const string PlaceholderPartition = "${partition}";
    public const string PlaceholderTopic = "${topic}";
}
