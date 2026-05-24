using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Smtp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Smtp.Tests;

public class SmtpSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SmtpSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSmtpSinkTask()
    {
        var connector = new SmtpSinkConnector();
        Assert.Equal(typeof(SmtpSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesHostConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.HostConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesUseSslConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.UseSslConfig);
    }

    [Fact]
    public void Config_DefinesUseStartTlsConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.UseStartTlsConfig);
    }

    [Fact]
    public void Config_DefinesTimeoutSecondsConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.TimeoutSecondsConfig);
    }

    [Fact]
    public void Config_DefinesAcceptInvalidCertificatesConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.AcceptInvalidCertificatesConfig);
    }

    [Fact]
    public void Config_DefinesFromAddressConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.FromAddressConfig);
    }

    [Fact]
    public void Config_DefinesFromNameConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.FromNameConfig);
    }

    [Fact]
    public void Config_DefinesReplyToConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.ReplyToConfig);
    }

    [Fact]
    public void Config_DefinesDefaultSubjectConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.DefaultSubjectConfig);
    }

    [Fact]
    public void Config_DefinesToFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.ToFieldConfig);
    }

    [Fact]
    public void Config_DefinesCcFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.CcFieldConfig);
    }

    [Fact]
    public void Config_DefinesBccFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BccFieldConfig);
    }

    [Fact]
    public void Config_DefinesSubjectFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.SubjectFieldConfig);
    }

    [Fact]
    public void Config_DefinesBodyFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BodyFieldConfig);
    }

    [Fact]
    public void Config_DefinesBodyHtmlFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BodyHtmlFieldConfig);
    }

    [Fact]
    public void Config_DefinesAttachmentsFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.AttachmentsFieldConfig);
    }

    [Fact]
    public void Config_DefinesHeadersFieldConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.HeadersFieldConfig);
    }

    [Fact]
    public void Config_DefinesBodyTemplateConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BodyTemplateConfig);
    }

    [Fact]
    public void Config_DefinesBodyHtmlTemplateConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BodyHtmlTemplateConfig);
    }

    [Fact]
    public void Config_DefinesSubjectTemplateConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.SubjectTemplateConfig);
    }

    [Fact]
    public void Config_DefinesSendAsHtmlConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.SendAsHtmlConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesRetryCountConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.RetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new SmtpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SmtpConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.HostConfig] = "smtp.example.com",
            [SmtpConnectorConfig.FromAddressConfig] = "test@example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SmtpConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenHostMissing()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.TopicsConfig] = "emails",
            [SmtpConnectorConfig.FromAddressConfig] = "test@example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SmtpConnectorConfig.HostConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenFromAddressMissing()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.TopicsConfig] = "emails",
            [SmtpConnectorConfig.HostConfig] = "smtp.example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SmtpConnectorConfig.FromAddressConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.TopicsConfig] = "emails",
            [SmtpConnectorConfig.HostConfig] = "smtp.example.com",
            [SmtpConnectorConfig.FromAddressConfig] = "test@example.com"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.TopicsConfig] = "emails",
            [SmtpConnectorConfig.HostConfig] = "smtp.example.com",
            [SmtpConnectorConfig.FromAddressConfig] = "test@example.com"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[SmtpConnectorConfig.HostConfig], taskConfigs[0][SmtpConnectorConfig.HostConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new SmtpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SmtpConnectorConfig.TopicsConfig] = "emails",
            [SmtpConnectorConfig.HostConfig] = "smtp.example.com",
            [SmtpConnectorConfig.FromAddressConfig] = "test@example.com"
        };

        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new SmtpSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_HostIsHighImportance()
    {
        var connector = new SmtpSinkConnector();
        var hostKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.HostConfig);
        Assert.Equal(Importance.High, hostKey.Importance);
    }

    [Fact]
    public void Config_FromAddressIsHighImportance()
    {
        var connector = new SmtpSinkConnector();
        var fromKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.FromAddressConfig);
        Assert.Equal(Importance.High, fromKey.Importance);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new SmtpSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_HasExpectedPortDefault()
    {
        var connector = new SmtpSinkConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.PortConfig);
        Assert.Equal(SmtpConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeoutDefault()
    {
        var connector = new SmtpSinkConnector();
        var timeoutKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.TimeoutSecondsConfig);
        Assert.Equal(SmtpConnectorConfig.DefaultTimeoutSeconds, timeoutKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new SmtpSinkConnector();
        var batchKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.BatchSizeConfig);
        Assert.Equal(SmtpConnectorConfig.DefaultBatchSize, batchKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryCountDefault()
    {
        var connector = new SmtpSinkConnector();
        var retryKey = connector.Config.Keys.First(k => k.Name == SmtpConnectorConfig.RetryCountConfig);
        Assert.Equal(SmtpConnectorConfig.DefaultRetryCount, retryKey.DefaultValue);
    }
}

public class SmtpSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new SmtpSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new SmtpSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_SkipsEmptyValues()
    {
        using var task = new SmtpSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new SmtpSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new SmtpSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new SmtpSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new SmtpSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
