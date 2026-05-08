using System.Diagnostics.CodeAnalysis;
using System.Text;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge;

/// <summary>
/// Task that consumes from Surgewave and produces to Kafka.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class KafkaBridgeSinkTask : SinkTask
{
    private IProducer<byte[], byte[]>? _producer;
    private string? _kafkaTopicOverride;
    private bool _topicMappingEnabled;
    private string _topicMappingPrefix = "";
    private string _topicMappingSuffix = "";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var kafkaServers = config[KafkaBridgeConnectorConfig.KafkaBootstrapServers];
        _kafkaTopicOverride = config.TryGetValue(KafkaBridgeConnectorConfig.Topic, out var kafkaTopic) ? kafkaTopic : null;
        _topicMappingEnabled = (config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingEnabled, out var topicMappingEnabled) ? topicMappingEnabled : "false") == "true";
        _topicMappingPrefix = config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingPrefix, out var topicMappingPrefix) ? topicMappingPrefix : "";
        _topicMappingSuffix = config.TryGetValue(KafkaBridgeConnectorConfig.TopicMappingSuffix, out var topicMappingSuffix) ? topicMappingSuffix : "";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaServers,
            LingerMs = int.Parse(config.TryGetValue(KafkaBridgeConnectorConfig.LingerMs, out var lingerMs)
                ? lingerMs : KafkaBridgeConnectorConfig.DefaultLingerMs.ToString()),
            BatchSize = int.Parse(config.TryGetValue(KafkaBridgeConnectorConfig.BatchSize, out var batchSize)
                ? batchSize : KafkaBridgeConnectorConfig.DefaultBatchSize.ToString())
        };

        // Acks
        var acks = config.TryGetValue(KafkaBridgeConnectorConfig.Acks, out var acksVal) ? acksVal : "all";
        producerConfig.Acks = acks switch
        {
            "0" => Confluent.Kafka.Acks.None,
            "1" => Confluent.Kafka.Acks.Leader,
            _ => Confluent.Kafka.Acks.All
        };

        // Compression
        var compression = config.TryGetValue(KafkaBridgeConnectorConfig.CompressionType, out var compressionType) ? compressionType : "none";
        producerConfig.CompressionType = compression?.ToLowerInvariant() switch
        {
            "gzip" => Confluent.Kafka.CompressionType.Gzip,
            "snappy" => Confluent.Kafka.CompressionType.Snappy,
            "lz4" => Confluent.Kafka.CompressionType.Lz4,
            "zstd" => Confluent.Kafka.CompressionType.Zstd,
            _ => Confluent.Kafka.CompressionType.None
        };

        // Security configuration
        if (config.TryGetValue(KafkaBridgeConnectorConfig.SecurityProtocol, out var protocol) && !string.IsNullOrEmpty(protocol))
        {
            producerConfig.SecurityProtocol = protocol.ToUpperInvariant() switch
            {
                "SSL" => SecurityProtocol.Ssl,
                "SASL_PLAINTEXT" => SecurityProtocol.SaslPlaintext,
                "SASL_SSL" => SecurityProtocol.SaslSsl,
                _ => SecurityProtocol.Plaintext
            };
        }

        if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslMechanism, out var mechanism) && !string.IsNullOrEmpty(mechanism))
        {
            producerConfig.SaslMechanism = mechanism.ToUpperInvariant() switch
            {
                "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
                _ => SaslMechanism.Plain
            };

            if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslUsername, out var username))
                producerConfig.SaslUsername = username;
            if (config.TryGetValue(KafkaBridgeConnectorConfig.SaslPassword, out var password))
                producerConfig.SaslPassword = password;
        }

        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var kafkaTopic = GetKafkaTopic(record.Topic);

            var message = new Message<byte[], byte[]>
            {
                Key = record.Key!,
                Value = record.Value,
                Headers = ConvertHeaders(record)
            };

            if (record.Timestamp != default)
            {
                message.Timestamp = new Timestamp(record.Timestamp);
            }

            await _producer!.ProduceAsync(kafkaTopic, message, cancellationToken);
        }
    }

    private string GetKafkaTopic(string surgewaveTopic)
    {
        string result;

        if (!string.IsNullOrEmpty(_kafkaTopicOverride))
        {
            result = _kafkaTopicOverride.Replace("${surgewave.topic}", surgewaveTopic);
        }
        else
        {
            result = surgewaveTopic;
        }

        if (_topicMappingEnabled)
        {
            if (!string.IsNullOrEmpty(_topicMappingPrefix))
                result = _topicMappingPrefix + result;
            if (!string.IsNullOrEmpty(_topicMappingSuffix))
                result = result + _topicMappingSuffix;
        }

        return result;
    }

    private static Headers ConvertHeaders(SinkRecord record)
    {
        var headers = new Headers();

        headers.Add("surgewave.source.topic", Encoding.UTF8.GetBytes(record.Topic));
        headers.Add("surgewave.source.partition", Encoding.UTF8.GetBytes(record.Partition.ToString()));
        headers.Add("surgewave.source.offset", Encoding.UTF8.GetBytes(record.Offset.ToString()));

        if (record.Headers != null)
        {
            foreach (var (key, value) in record.Headers)
            {
                headers.Add($"surgewave.header.{key}", value);
            }
        }

        return headers;
    }

    public override Task FlushAsync(IDictionary<Connect.TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        _producer?.Flush(cancellationToken);
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
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
