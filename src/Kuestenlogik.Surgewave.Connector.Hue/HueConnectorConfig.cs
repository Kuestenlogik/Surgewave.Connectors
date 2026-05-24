namespace Kuestenlogik.Surgewave.Connector.Hue;

/// <summary>
/// Configuration constants for Philips Hue connector.
/// </summary>
public static class HueConnectorConfig
{
    // Bridge settings
    public const string BridgeIp = "hue.bridge.ip";
    public const string AppKey = "hue.app.key";
    public const string ClientKey = "hue.client.key";

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncludeLights = "hue.include.lights";
    public const string IncludeSensors = "hue.include.sensors";
    public const string IncludeGroups = "hue.include.groups";
    public const string IncludeScenes = "hue.include.scenes";
    public const string EventsOnly = "hue.events.only";  // Only emit on state changes

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultLightId = "hue.default.light.id";
    public const string DefaultGroupId = "hue.default.group.id";
    public const string TransitionTimeMs = "hue.transition.time.ms";

    // Defaults
    public const int DefaultPollIntervalMs = 1000;
    public const int DefaultTransitionTimeMs = 400;
}
