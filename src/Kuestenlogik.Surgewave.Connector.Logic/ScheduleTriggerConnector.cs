using System.Text;
using System.Text.Json;
using Cronos;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Generates events on a cron schedule.
/// Useful for scheduled pipeline triggers.
/// </summary>
[ConnectorMetadata(
    Name = "Schedule Trigger",
    Description = "Generate events on a cron schedule",
    Author = "Surgewave",
    Tags = "trigger,schedule,cron,timer",
    Icon = "schedule")]
public sealed class ScheduleTriggerConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ScheduleTriggerTask);

    private string _cronExpression = "";
    private string _outputTopic = "";
    private string _payload = "";

    public override ConfigDef Config => new ConfigDef()
        .Define(ScheduleTriggerConfig.CronExpression, ConfigType.String, "", Importance.High,
            "Cron expression (e.g., '0 * * * *' for every hour, '*/5 * * * *' for every 5 minutes)", EditorHint.Cron)
        .Define(ScheduleTriggerConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send trigger events to", EditorHint.Topic)
        .Define(ScheduleTriggerConfig.Payload, ConfigType.String, "{}", Importance.Low,
            "JSON payload to include in trigger events", EditorHint.Multiline)
        .Define(ScheduleTriggerConfig.Timezone, ConfigType.String, "UTC", Importance.Low,
            "Timezone for cron evaluation (e.g., 'UTC', 'Europe/Berlin')");

    public override void Start(IDictionary<string, string> config)
    {
        _cronExpression = config.GetValueOrDefault(ScheduleTriggerConfig.CronExpression, "")
            ?? throw new ArgumentException("Cron expression is required");
        _outputTopic = config.GetValueOrDefault(ScheduleTriggerConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");
        _payload = config.GetValueOrDefault(ScheduleTriggerConfig.Payload, "{}") ?? "{}";

        // Validate cron expression
        try
        {
            CronExpression.Parse(_cronExpression);
        }
        catch (Exception ex)
        {
            throw new ArgumentException($"Invalid cron expression: {ex.Message}", ex);
        }
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return
        [
            new Dictionary<string, string>
            {
                [ScheduleTriggerConfig.CronExpression] = _cronExpression,
                [ScheduleTriggerConfig.OutputTopic] = _outputTopic,
                [ScheduleTriggerConfig.Payload] = _payload,
                [ScheduleTriggerConfig.Timezone] = "UTC"
            }
        ];
    }
}

/// <summary>
/// Task that generates events on a schedule.
/// </summary>
public sealed class ScheduleTriggerTask : SourceTask
{
    public override string Version => "1.0.0";

    private CronExpression? _cron;
    private string _outputTopic = "";
    private string _payload = "";
    private TimeZoneInfo _timezone = TimeZoneInfo.Utc;
    private DateTimeOffset _lastExecution = DateTimeOffset.MinValue;
    private DateTimeOffset? _nextExecution;

    public override void Start(IDictionary<string, string> config)
    {
        var cronStr = config.GetValueOrDefault(ScheduleTriggerConfig.CronExpression, "") ?? "";
        _cron = CronExpression.Parse(cronStr);
        _outputTopic = config.GetValueOrDefault(ScheduleTriggerConfig.OutputTopic, "") ?? "";
        _payload = config.GetValueOrDefault(ScheduleTriggerConfig.Payload, "{}") ?? "{}";

        var tzId = config.GetValueOrDefault(ScheduleTriggerConfig.Timezone, "UTC") ?? "UTC";
        try
        {
            _timezone = TimeZoneInfo.FindSystemTimeZoneById(tzId);
        }
        catch
        {
            _timezone = TimeZoneInfo.Utc;
        }

        // Calculate next execution
        _nextExecution = _cron.GetNextOccurrence(DateTimeOffset.UtcNow, _timezone);
    }

    public override void Stop()
    {
        _cron = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_cron == null || _nextExecution == null)
        {
            await Task.Delay(1000, cancellationToken);
            return [];
        }

        var now = DateTimeOffset.UtcNow;

        // Not yet time for next execution
        if (now < _nextExecution.Value)
        {
            var delay = _nextExecution.Value - now;
            if (delay > TimeSpan.FromSeconds(1))
            {
                delay = TimeSpan.FromSeconds(1); // Don't sleep too long
            }
            await Task.Delay(delay, cancellationToken);
            return [];
        }

        // Time to trigger
        _lastExecution = now;
        var scheduledTime = _nextExecution.Value;

        // Calculate next execution
        _nextExecution = _cron.GetNextOccurrence(now.AddSeconds(1), _timezone);

        // Build trigger event
        var eventData = new
        {
            trigger = "schedule",
            scheduled_time = scheduledTime.ToUnixTimeMilliseconds(),
            actual_time = now.ToUnixTimeMilliseconds(),
            next_execution = _nextExecution?.ToUnixTimeMilliseconds(),
            payload = JsonDocument.Parse(_payload).RootElement
        };

        var value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(eventData));
        var key = Encoding.UTF8.GetBytes(scheduledTime.ToString("o"));

        return
        [
            new SourceRecord
            {
                Topic = _outputTopic,
                Key = key,
                Value = value,
                Timestamp = now,
                SourcePartition = new Dictionary<string, object> { ["type"] = "schedule" },
                SourceOffset = new Dictionary<string, object> { ["last_execution"] = now.ToUnixTimeMilliseconds() }
            }
        ];
    }
}

/// <summary>
/// Configuration keys for ScheduleTriggerConnector.
/// </summary>
public static class ScheduleTriggerConfig
{
    public const string CronExpression = "schedule.cron";
    public const string OutputTopic = "output.topic";
    public const string Payload = "schedule.payload";
    public const string Timezone = "schedule.timezone";
}
