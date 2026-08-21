using System.Globalization;
using NATS.Client.Core;

namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// Builds <see cref="NatsOpts"/> from connector configuration so source and sink tasks
/// honour the same connection, authentication, TLS and reconnect settings.
/// </summary>
public static class NatsOptionsBuilder
{
    /// <summary>
    /// Translate the connector configuration into NATS client options.
    /// </summary>
    public static NatsOpts Build(IDictionary<string, string> config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var opts = new NatsOpts
        {
            Url = Get(config, NatsConnectorConfig.Url) ?? NatsConnectorConfig.DefaultUrl
        };

        opts = WithAuth(opts, config);

        if (GetBool(config, NatsConnectorConfig.UseTls, NatsConnectorConfig.DefaultUseTls))
        {
            opts = opts with { TlsOpts = NatsTlsOpts.Default with { Mode = TlsMode.Require } };
        }

        var reconnectWait = TimeSpan.FromMilliseconds(
            GetInt(config, NatsConnectorConfig.ReconnectWaitMs, NatsConnectorConfig.DefaultReconnectWaitMs));

        return opts with
        {
            ReconnectWaitMin = reconnectWait,
            // Keep the jittered backoff window valid when the configured wait exceeds the upper bound.
            ReconnectWaitMax = reconnectWait > opts.ReconnectWaitMax ? reconnectWait : opts.ReconnectWaitMax,
            MaxReconnectRetry = GetInt(config, NatsConnectorConfig.MaxReconnects, NatsConnectorConfig.DefaultMaxReconnects)
        };
    }

    private static NatsOpts WithAuth(NatsOpts opts, IDictionary<string, string> config)
    {
        if (Get(config, NatsConnectorConfig.CredentialsFile) is { } credentialsFile)
            return opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = credentialsFile } };

        if (Get(config, NatsConnectorConfig.Token) is { } token)
            return opts with { AuthOpts = NatsAuthOpts.Default with { Token = token } };

        if (Get(config, NatsConnectorConfig.Username) is { } username &&
            Get(config, NatsConnectorConfig.Password) is { } password)
        {
            return opts with { AuthOpts = NatsAuthOpts.Default with { Username = username, Password = password } };
        }

        return opts;
    }

    private static string? Get(IDictionary<string, string> config, string key)
        => config.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;

    private static bool GetBool(IDictionary<string, string> config, string key, bool defaultValue)
        => Get(config, key) is { } value ? bool.Parse(value) : defaultValue;

    private static int GetInt(IDictionary<string, string> config, string key, int defaultValue)
        => Get(config, key) is { } value ? int.Parse(value, CultureInfo.InvariantCulture) : defaultValue;
}
