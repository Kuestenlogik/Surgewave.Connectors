using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Akka.Actor;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka;

/// <summary>
/// Task that sends messages to Akka.NET actors.
/// Supports Tell (fire-and-forget) and Ask (request-response) patterns.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "ActorSystem disposed via Terminate() in Stop()")]
public sealed class AkkaSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private ActorSystem? _actorSystem;
    private ActorSelection? _targetActorSelection;
    private string _actorPath = "";
    private string _remoteAddress = "";
    private long _askTimeoutMs = AkkaConnectorConfig.DefaultAskTimeoutMs;
    private bool _tellOnly = true;
    private int _batchSize = AkkaConnectorConfig.DefaultBatchSize;
    private int _maxRetryCount = AkkaConnectorConfig.DefaultMaxRetryCount;
    private long _retryDelayMs = AkkaConnectorConfig.DefaultRetryDelayMs;

    public override void Start(IDictionary<string, string> config)
    {
        var systemName = GetConfigValue(config, AkkaConnectorConfig.ActorSystemNameConfig, AkkaConnectorConfig.DefaultActorSystemName);
        var hoconConfig = GetConfigValue(config, AkkaConnectorConfig.ActorSystemConfigConfig, "");
        _actorPath = config[AkkaConnectorConfig.ActorPathConfig];
        _remoteAddress = GetConfigValue(config, AkkaConnectorConfig.RemoteAddressConfig, "");
        _askTimeoutMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.AskTimeoutMsConfig, AkkaConnectorConfig.DefaultAskTimeoutMs.ToString()));
        _tellOnly = bool.Parse(GetConfigValue(config, AkkaConnectorConfig.TellOnlyConfig, "true"));
        _batchSize = int.Parse(GetConfigValue(config, AkkaConnectorConfig.BatchSizeConfig, AkkaConnectorConfig.DefaultBatchSize.ToString()));
        _maxRetryCount = int.Parse(GetConfigValue(config, AkkaConnectorConfig.MaxRetryCountConfig, AkkaConnectorConfig.DefaultMaxRetryCount.ToString()));
        _retryDelayMs = long.Parse(GetConfigValue(config, AkkaConnectorConfig.RetryDelayMsConfig, AkkaConnectorConfig.DefaultRetryDelayMs.ToString()));

        // Create actor system
        if (!string.IsNullOrEmpty(hoconConfig))
        {
            var akkaConfig = global::Akka.Configuration.ConfigurationFactory.ParseString(hoconConfig);
            _actorSystem = ActorSystem.Create(systemName, akkaConfig);
        }
        else
        {
            _actorSystem = ActorSystem.Create(systemName);
        }

        // Build actor selection path
        var fullPath = !string.IsNullOrEmpty(_remoteAddress)
            ? $"{_remoteAddress}{_actorPath}"
            : _actorPath;

        _targetActorSelection = _actorSystem.ActorSelection(fullPath);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    public override void Stop()
    {
        if (_actorSystem != null)
        {
            _actorSystem.Terminate().Wait(TimeSpan.FromSeconds(5));
            _actorSystem.Dispose();
            _actorSystem = null;
        }
        _targetActorSelection = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_actorSystem == null || _targetActorSelection == null || records.Count == 0)
            return;

        // Process in batches
        foreach (var batch in records.Chunk(_batchSize))
        {
            var tasks = new List<Task>();

            foreach (var record in batch)
            {
                // Skip tombstones
                if (record.Value == null || record.Value.Length == 0)
                    continue;

                var task = SendMessageWithRetryAsync(record, cancellationToken);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }

    private async Task SendMessageWithRetryAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var message = CreateMessage(record);
        var retries = 0;

        while (retries <= _maxRetryCount)
        {
            try
            {
                if (_tellOnly)
                {
                    _targetActorSelection!.Tell(message);
                    return;
                }
                else
                {
                    var timeout = TimeSpan.FromMilliseconds(_askTimeoutMs);
                    await _targetActorSelection!.Ask<object>(message, timeout, cancellationToken);
                    return;
                }
            }
            catch (AskTimeoutException) when (retries < _maxRetryCount)
            {
                retries++;
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retries), cancellationToken);
            }
            catch (Exception) when (retries < _maxRetryCount)
            {
                retries++;
                await Task.Delay(TimeSpan.FromMilliseconds(_retryDelayMs * retries), cancellationToken);
            }
        }
    }

    private SurgewaveMessage CreateMessage(SinkRecord record)
    {
        // Try to deserialize as JSON
        try
        {
            var content = Encoding.UTF8.GetString(record.Value);

            if (content.StartsWith('{') || content.StartsWith('['))
            {
                var parsed = JsonSerializer.Deserialize<Dictionary<string, object?>>(content);
                if (parsed != null)
                {
                    // Wrap with metadata
                    return new SurgewaveMessage
                    {
                        Topic = record.Topic,
                        Partition = record.Partition,
                        Offset = record.Offset,
                        Timestamp = record.Timestamp,
                        Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                        Data = parsed,
                        Headers = record.Headers?.ToDictionary(
                            h => h.Key,
                            h => Encoding.UTF8.GetString(h.Value))
                    };
                }
            }

            // Plain text message
            return new SurgewaveMessage
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Timestamp = record.Timestamp,
                Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                Data = content,
                Headers = record.Headers?.ToDictionary(
                    h => h.Key,
                    h => Encoding.UTF8.GetString(h.Value))
            };
        }
        catch
        {
            // Fallback to raw bytes
            return new SurgewaveMessage
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Timestamp = record.Timestamp,
                Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                Data = Convert.ToBase64String(record.Value),
                Headers = record.Headers?.ToDictionary(
                    h => h.Key,
                    h => Encoding.UTF8.GetString(h.Value))
            };
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Tell operations are fire-and-forget, Ask operations complete in PutAsync
        return Task.CompletedTask;
    }
}

/// <summary>
/// Message wrapper sent to Akka actors from Surgewave.
/// </summary>
public sealed class SurgewaveMessage
{
    /// <summary>
    /// Source Surgewave topic.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Source partition.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Source offset.
    /// </summary>
    public long Offset { get; init; }

    /// <summary>
    /// Message timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Message key (optional).
    /// </summary>
    public string? Key { get; init; }

    /// <summary>
    /// Message data (deserialized JSON or string).
    /// </summary>
    public required object Data { get; init; }

    /// <summary>
    /// Message headers (optional).
    /// </summary>
    public Dictionary<string, string>? Headers { get; init; }
}
