namespace Kuestenlogik.Surgewave.Connector.Alexa;

/// <summary>
/// Configuration constants for Amazon Alexa connector.
/// </summary>
public static class AlexaConnectorConfig
{
    // Authentication
    public const string ClientId = "alexa.client.id";
    public const string ClientSecret = "alexa.client.secret";
    public const string RefreshToken = "alexa.refresh.token";
    public const string Region = "alexa.region";  // NA, EU, FE

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncludeLights = "alexa.include.lights";
    public const string IncludeSwitches = "alexa.include.switches";
    public const string IncludeThermostats = "alexa.include.thermostats";
    public const string IncludeLocks = "alexa.include.locks";
    public const string IncludeSensors = "alexa.include.sensors";
    public const string EventsOnly = "alexa.events.only";
    public const string FilterEndpointIds = "alexa.filter.endpoint.ids";

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultEndpointId = "alexa.default.endpoint.id";

    // Defaults
    public const int DefaultPollIntervalMs = 5000;
    public const string DefaultRegion = "NA";
}
