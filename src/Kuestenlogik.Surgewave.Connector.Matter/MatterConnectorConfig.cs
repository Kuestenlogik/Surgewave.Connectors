namespace Kuestenlogik.Surgewave.Connector.Matter;

/// <summary>
/// Configuration constants for Matter connector.
/// </summary>
public static class MatterConnectorConfig
{
    // Controller connection
    public const string ControllerUrl = "matter.controller.url";
    public const string ApiKey = "matter.api.key";
    public const string CommissionerNodeId = "matter.commissioner.node.id";

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncludeLighting = "matter.include.lighting";
    public const string IncludeSensors = "matter.include.sensors";
    public const string IncludeSwitches = "matter.include.switches";
    public const string IncludeThermostat = "matter.include.thermostat";
    public const string IncludeDoorLock = "matter.include.doorlock";
    public const string EventsOnly = "matter.events.only";
    public const string FilterNodeIds = "matter.filter.node.ids";  // Comma-separated node IDs

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultNodeId = "matter.default.node.id";
    public const string DefaultEndpointId = "matter.default.endpoint.id";

    // Defaults
    public const int DefaultPollIntervalMs = 1000;
    public const int DefaultEndpointIdValue = 1;
}
