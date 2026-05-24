using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Mirror.Filters;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;
using Microsoft.Extensions.Logging;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Source connector for cross-cluster replication.
/// Reads from a source cluster and produces to the target cluster.
/// Supports dynamic topic discovery - newly created topics matching the configured
/// pattern are automatically picked up on the next refresh interval.
/// </summary>
public sealed class MirrorSourceConnector : SourceConnector
{
    private MirrorMakerConfig _config = null!;
    private IReplicationPolicy _policy = null!;
    private DefaultTopicFilter _topicFilter = null!;
    private List<string> _sourceTopics = [];
    private Timer? _refreshTimer;
    private SurgewaveNativeClient? _sourceClient;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private bool _isConnected;
    private ILogger? _logger;

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MirrorSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("source.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the source cluster")
        .Define("target.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the target cluster")
        .Define("source.bootstrap.servers", ConfigType.String, Importance.High,
            "Bootstrap servers for source cluster")
        .Define("target.bootstrap.servers", ConfigType.String, Importance.High,
            "Bootstrap servers for target cluster")
        .Define("topics", ConfigType.String, ".*", Importance.Medium,
            "Regex pattern for topics to replicate", EditorHint.Topic)
        .Define("topics.whitelist", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of topics to replicate (if specified, only these topics are replicated)")
        .Define("topics.blacklist", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of topics to exclude from replication")
        .Define("tasks.max", ConfigType.Int, 1L, Importance.Medium,
            "Maximum number of tasks")
        .Define("sync.topic.configs.enabled", ConfigType.Boolean, true, Importance.Low,
            "Sync topic configurations to target cluster")
        .Define("replication.policy.class", ConfigType.String,
            "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy", Importance.Low,
            "Replication policy class for topic naming")
        .Define("replication.policy.separator", ConfigType.String, ".", Importance.Low,
            "Separator for topic naming in replication policy")
        .Define("source.security.protocol", ConfigType.String, null, Importance.Medium,
            "Security protocol for source cluster", EditorHint.Select, options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
        .Define("source.sasl.mechanism", ConfigType.String, null, Importance.Medium,
            "SASL mechanism for source cluster", EditorHint.Select, options: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"])
        .Define("target.security.protocol", ConfigType.String, null, Importance.Medium,
            "Security protocol for target cluster", EditorHint.Select, options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
        .Define("target.sasl.mechanism", ConfigType.String, null, Importance.Medium,
            "SASL mechanism for target cluster", EditorHint.Select, options: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"])
        .Define("consumer.poll.timeout.ms", ConfigType.Int, 1000L, Importance.Low,
            "Consumer poll timeout in milliseconds")
        .Define("fetch.max.bytes", ConfigType.Int, 52428800L, Importance.Low,
            "Maximum bytes to fetch per poll")
        .Define("max.poll.records", ConfigType.Int, 500L, Importance.Low,
            "Maximum records per poll")
        .Define("topic.refresh.interval.ms", ConfigType.Int, 30000L, Importance.Low,
            "Interval for refreshing topic list from source cluster");

    public override void Start(IDictionary<string, string> config)
    {
        _config = MirrorMakerConfig.FromDictionary(config);
        _logger = Context.Logger;

        // Validate required configuration
        if (string.IsNullOrEmpty(_config.SourceBootstrapServers))
            throw new ArgumentException("source.bootstrap.servers is required");

        if (string.IsNullOrEmpty(_config.TargetBootstrapServers))
            throw new ArgumentException("target.bootstrap.servers is required");

        _policy = ReplicationPolicyFactory.Create(_config);
        _topicFilter = DefaultTopicFilter.FromConfig(_config, _policy);

        // Connect to source cluster for topic discovery
        ConnectToSourceClusterAsync().GetAwaiter().GetResult();

        // Discover topics from source cluster
        RefreshSourceTopicsAsync().GetAwaiter().GetResult();

        // Set up periodic topic refresh
        _refreshTimer = new Timer(
            _ => RefreshTopicsAndReconfigureAsync(),
            null,
            TimeSpan.FromMilliseconds(_config.TopicRefreshIntervalMs),
            TimeSpan.FromMilliseconds(_config.TopicRefreshIntervalMs));
    }

    private async Task ConnectToSourceClusterAsync()
    {
        try
        {
            var (host, port) = ParseBootstrapServer(_config.SourceBootstrapServers);
            _sourceClient = new SurgewaveNativeClient(host, port);
            await _sourceClient.ConnectAsync();
            _isConnected = true;
            _logger?.LogInformation("Connected to source cluster at {Host}:{Port}", host, port);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to connect to source cluster for topic discovery. Using configured topics only.");
            _isConnected = false;
        }
    }

    private static (string host, int port) ParseBootstrapServer(string bootstrapServers)
    {
        // Take the first server from the list
        var server = bootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? "localhost:9092";

        var parts = server.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092;

        return (host, port);
    }

    public override void Stop()
    {
        _refreshTimer?.Dispose();
        _refreshTimer = null;

        // Dispose source client connection
        if (_sourceClient != null)
        {
            _sourceClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _sourceClient = null;
            _isConnected = false;
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        if (_sourceTopics.Count == 0)
            return [];

        // Partition topics across tasks
        var tasksCount = Math.Min(maxTasks, _sourceTopics.Count);
        var partitionedTopics = PartitionTopics(_sourceTopics, tasksCount);

        return partitionedTopics.Select((topics, i) =>
        {
            var taskConfig = new Dictionary<string, string>
            {
                ["task.id"] = i.ToString(),
                ["topics"] = string.Join(",", topics),
                ["source.bootstrap.servers"] = _config.SourceBootstrapServers,
                ["target.bootstrap.servers"] = _config.TargetBootstrapServers,
                ["source.cluster.alias"] = _config.SourceClusterAlias,
                ["target.cluster.alias"] = _config.TargetClusterAlias,
                ["replication.policy.class"] = _config.ReplicationPolicyClass,
                ["replication.policy.separator"] = _config.ReplicationPolicySeparator,
                ["consumer.poll.timeout.ms"] = _config.ConsumerPollTimeoutMs.ToString(),
                ["fetch.max.bytes"] = _config.FetchMaxBytes.ToString(),
                ["fetch.min.bytes"] = _config.FetchMinBytes.ToString(),
                ["max.poll.records"] = _config.MaxPollRecords.ToString()
            };

            // Add security config if present
            if (!string.IsNullOrEmpty(_config.SourceSecurityProtocol))
                taskConfig["source.security.protocol"] = _config.SourceSecurityProtocol;
            if (!string.IsNullOrEmpty(_config.SourceSaslMechanism))
                taskConfig["source.sasl.mechanism"] = _config.SourceSaslMechanism;
            if (!string.IsNullOrEmpty(_config.SourceSaslUsername))
                taskConfig["source.sasl.username"] = _config.SourceSaslUsername;
            if (!string.IsNullOrEmpty(_config.SourceSaslPassword))
                taskConfig["source.sasl.password"] = _config.SourceSaslPassword;
            if (!string.IsNullOrEmpty(_config.TargetSecurityProtocol))
                taskConfig["target.security.protocol"] = _config.TargetSecurityProtocol;
            if (!string.IsNullOrEmpty(_config.TargetSaslMechanism))
                taskConfig["target.sasl.mechanism"] = _config.TargetSaslMechanism;

            return taskConfig;
        }).ToList();
    }

    private async Task RefreshSourceTopicsAsync()
    {
        await _refreshLock.WaitAsync();
        try
        {
            List<string> discoveredTopics;

            // Try to discover topics from source cluster
            if (_isConnected && _sourceClient != null)
            {
                try
                {
                    var topics = await _sourceClient.Topics.ListAsync();
                    discoveredTopics = topics.Select(t => t.Name).ToList();
                    _logger?.LogDebug("Discovered {Count} topics from source cluster", discoveredTopics.Count);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to list topics from source cluster, attempting reconnect");

                    // Try to reconnect
                    await ReconnectToSourceClusterAsync();

                    // Fall back to configured topics if reconnect also fails
                    if (!_isConnected)
                    {
                        discoveredTopics = GetConfiguredTopics();
                    }
                    else
                    {
                        // Retry after reconnect
                        try
                        {
                            var topics = await _sourceClient!.Topics.ListAsync();
                            discoveredTopics = topics.Select(t => t.Name).ToList();
                        }
                        catch
                        {
                            discoveredTopics = GetConfiguredTopics();
                        }
                    }
                }
            }
            else
            {
                // Not connected - use configured topics only
                discoveredTopics = GetConfiguredTopics();
            }

            // Apply topic filter to discovered topics
            _sourceTopics = _topicFilter.FilterTopics(discoveredTopics).ToList();
            _logger?.LogDebug("After filtering: {Count} topics to replicate", _sourceTopics.Count);
        }
        catch (Exception ex)
        {
            Context.RaiseError(new Exception($"Failed to refresh topics: {ex.Message}", ex));
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private List<string> GetConfiguredTopics()
    {
        // Fall back to whitelist if specified
        if (_config.TopicsWhitelist.Count > 0)
        {
            return _config.TopicsWhitelist.ToList();
        }

        // Return empty list - pattern matching will be applied to actual topics
        // when they are discovered
        return [];
    }

    private async Task ReconnectToSourceClusterAsync()
    {
        try
        {
            // Dispose old client
            if (_sourceClient != null)
            {
                await _sourceClient.DisposeAsync();
                _sourceClient = null;
            }

            // Create new connection
            await ConnectToSourceClusterAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to reconnect to source cluster");
            _isConnected = false;
        }
    }

    private async void RefreshTopicsAndReconfigureAsync()
    {
        var previousTopics = _sourceTopics.ToHashSet();
        await RefreshSourceTopicsAsync();

        // Request reconfiguration if topics changed
        var currentTopics = _sourceTopics.ToHashSet();
        if (!currentTopics.SetEquals(previousTopics))
        {
            var added = currentTopics.Except(previousTopics).ToList();
            var removed = previousTopics.Except(currentTopics).ToList();

            if (added.Count > 0)
                _logger?.LogInformation("New topics discovered: {Topics}", string.Join(", ", added));
            if (removed.Count > 0)
                _logger?.LogInformation("Topics removed: {Topics}", string.Join(", ", removed));

            Context.RequestTaskReconfiguration();
        }
    }

    private static List<List<string>> PartitionTopics(List<string> topics, int partitions)
    {
        var result = new List<List<string>>();
        for (int i = 0; i < partitions; i++)
        {
            result.Add([]);
        }

        for (int i = 0; i < topics.Count; i++)
        {
            result[i % partitions].Add(topics[i]);
        }

        return result.Where(p => p.Count > 0).ToList();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _refreshTimer?.Dispose();
            _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _refreshLock.Dispose();
        }
        base.Dispose(disposing);
    }
}
