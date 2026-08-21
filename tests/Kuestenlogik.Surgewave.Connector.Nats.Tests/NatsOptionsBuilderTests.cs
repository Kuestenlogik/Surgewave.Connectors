using Kuestenlogik.Surgewave.Connector.Nats;
using NATS.Client.Core;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class NatsOptionsBuilderTests
{
    [Fact]
    public void Build_WithoutOverrides_UsesDefaultUrlAndNoForcedTls()
    {
        var opts = NatsOptionsBuilder.Build(new Dictionary<string, string>());

        Assert.Equal(NatsConnectorConfig.DefaultUrl, opts.Url);
        Assert.NotEqual(TlsMode.Require, opts.TlsOpts.Mode);
    }

    [Fact]
    public void Build_WithTlsEnabled_RequiresTls()
    {
        var opts = NatsOptionsBuilder.Build(new Dictionary<string, string>
        {
            [NatsConnectorConfig.UseTls] = "true"
        });

        Assert.Equal(TlsMode.Require, opts.TlsOpts.Mode);
    }

    [Fact]
    public void Build_WithReconnectSettings_AppliesThem()
    {
        var opts = NatsOptionsBuilder.Build(new Dictionary<string, string>
        {
            [NatsConnectorConfig.ReconnectWaitMs] = "45000",
            [NatsConnectorConfig.MaxReconnects] = "7"
        });

        Assert.Equal(TimeSpan.FromMilliseconds(45000), opts.ReconnectWaitMin);
        Assert.True(opts.ReconnectWaitMax >= opts.ReconnectWaitMin);
        Assert.Equal(7, opts.MaxReconnectRetry);
    }

    [Fact]
    public void Build_WithToken_UsesTokenAuthentication()
    {
        var opts = NatsOptionsBuilder.Build(new Dictionary<string, string>
        {
            [NatsConnectorConfig.Url] = "nats://example:4222",
            [NatsConnectorConfig.Token] = "s3cret"
        });

        Assert.Equal("nats://example:4222", opts.Url);
        Assert.Equal("s3cret", opts.AuthOpts.Token);
    }

    [Fact]
    public void Build_WithEmptyAuthValues_LeavesAuthenticationUnset()
    {
        var opts = NatsOptionsBuilder.Build(new Dictionary<string, string>
        {
            [NatsConnectorConfig.CredentialsFile] = "",
            [NatsConnectorConfig.Token] = "",
            [NatsConnectorConfig.Username] = "",
            [NatsConnectorConfig.Password] = ""
        });

        Assert.Null(opts.AuthOpts.Token);
        Assert.Null(opts.AuthOpts.CredsFile);
    }
}
