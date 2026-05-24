using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge;

/// <summary>
/// Task that consumes from Kafka and produces to Surgewave.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
[SuppressMessage("Performance", "CA1823:Avoid unused private fields", Justification = "Reserved for future use")]
public sealed class KafkaBridgeSourceTask : SourceTask
{
    private IConsumer<byte[], byte[]>? _consumer;
    private string _surgewaveTopicTemplate = null!;
    private bool _topicMappingEnabled;
    private string _topicMappingPrefix = "";
    private string _topicMappingSuffix = "";
    private int _pollTimeoutMs;
#pragma warning disable CS0169 // Field is never used
    private long _messageId;
#pragma warning restore CS0169

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var kafkaServers = config[KafkaBridgeConnectorConfig.KafkaBootstrapServers];
        var groupId = config.TryGetValue(KafkaBridgeConnectorConfig.KafkaGroupId, out var grpId)
            ? grpId : KafkaBridgeConnectorConfig.DefaultGroupId;
        _surgewaveTopicTemplate = config[KafkaBridgeConnectorConfig.Topic];
        _topicMappingEnabled = (config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingEnabled, out var topicMappingEnabled) ? topicMappingEnabled : "false") == "true";
        _topicMappingPrefix = config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingPrefix, out var topicMappingPrefix) ? topicMappingPrefix : "";
        _topicMappingSuffix = config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingSuffix, out var topicMappingSuffix) ? topicMappingSuffix : "";
        _pollTimeoutMs = int.Parse(config.TryGetValue(KafkaBridgeConnectorConfig.PollTimeoutMs, out var pollTimeoutMs)
            ? pollTimeoutMs : KafkaBridgeConnectorConfig.DefaultPollTimeoutMs.ToString());

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaServers,
            GroupId = groupId,
            AutoOffsetReset = (config.TryGetValue(KafkaBridgeConnectorConfig.AutoOffsetReset, out var autoOffsetReset) ? autoOffsetReset : "earliest") == "latest"
                ? Confluent.Kafka.AutoOffsetReset.Latest
                : Confluent.Kafka.AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            MaxPollIntervalMs = 300000
        };

        // Security configuration
        if (config.TryGetValue(KafkaBridgeConnectorConfig.SecurityProtocol, out var protocol) && !string.IsNullOrEmpty(protocol))
        {
            consumerConfig.SecurityProtocol = protocol.ToUpperInvariant() switch
            {
                "SSL" => SecurityProtocol.Ssl,
                "SASL_PLAINTEXT" => SecurityProtocol.SaslPlaintext,
                "SASL_SSL" => SecurityProtocol.SaslSsl,
                _ => SecurityProtocol.Plaintext
            };
        }

        if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslMechanism, out var mechanism) && !string.IsNullOrEmpty(mechanism))
        {
            consumerConfig.SaslMechanism = mechanism.ToUpperInvariant() switch
            {
                "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
                _ => SaslMechanism.Plain
            };

            if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslUsername, out var username))
                consumerConfig.SaslUsername = username;
            if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslPassword, out var password))
                consumerConfig.SaslPassword = password;
        }

        _consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();

        // Subscribe to topics
        if (config.TryGetValue(KafkaBridgeConnectorConfig.Topics, out var topicsStr) && !string.IsNullOrWhiteSpace(topicsStr))
        {
            var topics = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            _consumer.Subscribe(topics);
        }
        else if (config.TryGetValue(KafkaBridgeConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern))
        {
            // Note: Confluent.Kafka doesn't support regex subscription directly.
            // For pattern-based subscription, use the topics.pattern config and let Kafka handle it.
            throw new NotSupportedException("Topic pattern subscription requires configuration at consumer config level. Use 'topics' config instead.");
        }
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        try
        {
            var result = _consumer!.Consume(TimeSpan.FromMilliseconds(_pollTimeoutMs));
            if (result != null && !result.IsPartitionEOF)
            {
                var surgewaveTopic = GetSurgewaveTopic(result.Topic);

                var record = new SourceRecord
                {
                    SourcePartition = new Dictionary<string, object>
                    {
                        ["kafka.topic"] = result.Topic,
                        ["kafka.partition"] = result.Partition.Value
                    },
                    SourceOffset = new Dictionary<string, object>
                    {
                        ["kafka.offset"] = result.Offset.Value
                    },
                    Topic = surgewaveTopic,
                    Partition = result.Partition.Value,
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Timestamp = result.Message.Timestamp.Type != TimestampType.NotAvailable
                        ? DateTimeOffset.FromUnixTimeMilliseconds(result.Message.Timestamp.UnixTimestampMs)
                        : DateTimeOffset.UtcNow,
                    Headers = ConvertHeaders(result.Message.Headers, result.Topic, result.Partition.Value, result.Offset.Value)
                };

                records.Add(record);
            }
        }
        catch (ConsumeException)
        {
            // Log and continue
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    private string GetSurgewaveTopic(string kafkaTopic)
    {
        var result = _surgewaveTopicTemplate.Replace("${kafka.topic}", kafkaTopic);

        if (_topicMappingEnabled)
        {
            if (!string.IsNullOrEmpty(_topicMappingPrefix))
                result = _topicMappingPrefix + result;
            if (!string.IsNullOrEmpty(_topicMappingSuffix))
                result = result + _topicMappingSuffix;
        }

        return result;
    }

    private static Dictionary<string, byte[]> ConvertHeaders(Headers? kafkaHeaders, string topic, int partition, long offset)
    {
        var headers = new Dictionary<string, byte[]>
        {
            ["kafka.source.topic"] = Encoding.UTF8.GetBytes(topic),
            ["kafka.source.partition"] = Encoding.UTF8.GetBytes(partition.ToString()),
            ["kafka.source.offset"] = Encoding.UTF8.GetBytes(offset.ToString())
        };

        if (kafkaHeaders != null)
        {
            foreach (var header in kafkaHeaders)
            {
                headers[$"kafka.header.{header.Key}"] = header.GetValueBytes();
            }
        }

        return headers;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        if (record.SourcePartition != null &&
            record.SourcePartition.TryGetValue("kafka.topic", out var topic) &&
            record.SourcePartition.TryGetValue("kafka.partition", out var partition) &&
            record.SourceOffset != null &&
            record.SourceOffset.TryGetValue("kafka.offset", out var offset))
        {
            var tp = new TopicPartitionOffset(
                topic.ToString()!,
                new Partition(Convert.ToInt32(partition)),
                new Offset(Convert.ToInt64(offset) + 1));

            _consumer?.Commit([tp]);
        }
    }

    public override void Stop()
    {
        _consumer?.Close();
        _consumer?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
