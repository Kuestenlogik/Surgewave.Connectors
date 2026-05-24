using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.HttpServer;

/// <summary>
/// A source connector that runs an HTTP server to receive requests.
/// Incoming HTTP requests are converted to records and produced to a topic.
/// </summary>
[ConnectorMetadata(
    Name = "HTTP Server Source",
    Description = "Runs an HTTP server that receives POST/PUT requests and produces them as messages to a topic.",
    Author = "KL Surgewave",
    Tags = "http,server,rest,api,webhook,ingest",
    Icon = "Server")]
public sealed class HttpServerSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(HttpServerSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Server settings
        .Define(HttpServerConnectorConfig.Host, ConfigType.String, HttpServerConnectorConfig.DefaultHost, Importance.Medium,
            "Host/IP to bind the HTTP server to")
        .Define(HttpServerConnectorConfig.Port, ConfigType.Int, HttpServerConnectorConfig.DefaultPort, Importance.High,
            "Port to listen on")
        .Define(HttpServerConnectorConfig.BasePath, ConfigType.String, HttpServerConnectorConfig.DefaultBasePath, Importance.Medium,
            "Base path prefix for all endpoints")

        // Source settings
        .Define(HttpServerConnectorConfig.SourceTopic, ConfigType.String, Importance.High,
            "Topic to produce incoming requests to")
        .Define(HttpServerConnectorConfig.SourcePath, ConfigType.String, HttpServerConnectorConfig.DefaultSourcePath, Importance.Medium,
            "Path to receive requests on (relative to base path)")
        .Define(HttpServerConnectorConfig.SourceMethods, ConfigType.String, HttpServerConnectorConfig.DefaultSourceMethods, Importance.Medium,
            "Allowed HTTP methods (comma-separated: POST,PUT)")
        .Define(HttpServerConnectorConfig.SourceIncludeHeaders, ConfigType.Boolean, true, Importance.Low,
            "Include request headers in the message")
        .Define(HttpServerConnectorConfig.SourceIncludeQueryParams, ConfigType.Boolean, true, Importance.Low,
            "Include query parameters in the message")

        // CORS
        .Define(HttpServerConnectorConfig.EnableCors, ConfigType.Boolean, false, Importance.Low,
            "Enable CORS support")
        .Define(HttpServerConnectorConfig.CorsOrigins, ConfigType.String, "*", Importance.Low,
            "Allowed CORS origins (comma-separated, or * for all)")

        // Authentication
        .Define(HttpServerConnectorConfig.AuthEnabled, ConfigType.Boolean, false, Importance.Medium,
            "Enable authentication for incoming requests")
        .Define(HttpServerConnectorConfig.AuthType, ConfigType.String, HttpServerConnectorConfig.AuthTypeNone, Importance.Medium,
            "Authentication type: none, api_key, or basic")
        .Define(HttpServerConnectorConfig.AuthApiKeys, ConfigType.Password, "", Importance.Medium,
            "Comma-separated list of valid API keys")
        .Define(HttpServerConnectorConfig.AuthApiKeyHeader, ConfigType.String, HttpServerConnectorConfig.DefaultApiKeyHeader, Importance.Low,
            "Header name for API key authentication")
        .Define(HttpServerConnectorConfig.AuthBasicUsers, ConfigType.Password, "", Importance.Medium,
            "Basic auth users in format user1:pass1,user2:pass2");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        // Validate required settings
        if (!config.TryGetValue(HttpServerConnectorConfig.SourceTopic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {HttpServerConnectorConfig.SourceTopic}");
        }

        // Validate port
        var port = config.TryGetValue(HttpServerConnectorConfig.Port, out var portStr) && int.TryParse(portStr, out var p)
            ? p : HttpServerConnectorConfig.DefaultPort;
        if (port < 1 || port > 65535)
        {
            throw new ArgumentException($"Invalid port: {port}. Must be between 1 and 65535.");
        }

        // Validate auth config
        var authEnabled = config.TryGetValue(HttpServerConnectorConfig.AuthEnabled, out var ae) && bool.TryParse(ae, out var enabled) && enabled;
        if (authEnabled)
        {
            var authType = config.TryGetValue(HttpServerConnectorConfig.AuthType, out var at) ? at : HttpServerConnectorConfig.AuthTypeNone;
            ValidateAuthConfig(authType, config);
        }
    }

    private static void ValidateAuthConfig(string authType, IDictionary<string, string> config)
    {
        switch (authType)
        {
            case HttpServerConnectorConfig.AuthTypeApiKey:
                if (!config.TryGetValue(HttpServerConnectorConfig.AuthApiKeys, out var keys) || string.IsNullOrEmpty(keys))
                    throw new ArgumentException($"API key auth requires {HttpServerConnectorConfig.AuthApiKeys}");
                break;

            case HttpServerConnectorConfig.AuthTypeBasic:
                if (!config.TryGetValue(HttpServerConnectorConfig.AuthBasicUsers, out var users) || string.IsNullOrEmpty(users))
                    throw new ArgumentException($"Basic auth requires {HttpServerConnectorConfig.AuthBasicUsers}");
                break;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - one HTTP server instance
        return [new Dictionary<string, string>(_config)];
    }
}
