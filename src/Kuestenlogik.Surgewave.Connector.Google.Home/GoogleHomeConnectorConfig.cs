namespace Kuestenlogik.Surgewave.Connector.Google.Home;

/// <summary>
/// Configuration constants for Google Home connector.
/// </summary>
public static class GoogleHomeConnectorConfig
{
    // Authentication
    public const string ServiceAccountJson = "google.service.account.json";
    public const string ServiceAccountFile = "google.service.account.file";
    public const string AgentUserId = "google.agent.user.id";

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncludeLights = "google.include.lights";
    public const string IncludeSwitches = "google.include.switches";
    public const string IncludeThermostats = "google.include.thermostats";
    public const string IncludeLocks = "google.include.locks";
    public const string IncludeSensors = "google.include.sensors";
    public const string EventsOnly = "google.events.only";
    public const string FilterDeviceIds = "google.filter.device.ids";

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultDeviceId = "google.default.device.id";

    // Defaults
    public const int DefaultPollIntervalMs = 5000;
}
