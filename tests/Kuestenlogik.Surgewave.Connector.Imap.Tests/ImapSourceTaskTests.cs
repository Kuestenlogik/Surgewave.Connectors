using Kuestenlogik.Surgewave.Connector.Imap;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Imap.Tests;

public class ImapSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new ImapSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void CurrentOffset_ReturnsNullBeforePoll()
    {
        using var task = new ImapSourceTask();
        Assert.Null(task.CurrentOffset);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new ImapSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new ImapSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new ImapSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesHostConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.PasswordConfig] = "password123"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesPortConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.PortConfig] = "993"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesUseSslConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.UseSslConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesTimeoutConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.TimeoutSecondsConfig] = "60"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesAcceptInvalidCertificatesConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.AcceptInvalidCertificatesConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesFolderConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.FolderConfig] = "Archive"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesBatchSizeConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.BatchSizeConfig] = "50"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesUseIdleConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.UseIdleConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesMarkAsReadConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.MarkAsReadConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesDeleteAfterReadConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.DeleteAfterReadConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesMoveAfterReadConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.MoveAfterReadConfig] = "true",
            [ImapConnectorConfig.MoveToFolderConfig] = "Archive"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesStartFromConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.StartFromConfig] = "earliest"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesUnseenOnlyConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.UnseenOnlyConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesSinceConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.SinceConfig] = "2024-01-01"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesSubjectFilterConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.SubjectFilterConfig] = "Invoice"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesFromFilterConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.FromFilterConfig] = "billing@example.com"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesIncludeBodyConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.IncludeBodyConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesIncludeAttachmentsConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.IncludeAttachmentsConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesMaxAttachmentSizeConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.MaxAttachmentSizeBytesConfig] = "5242880"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_ParsesPreferHtmlConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.PreferHtmlConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_IgnoresInvalidPortValue()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.PortConfig] = "invalid"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_IgnoresInvalidBooleanValues()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.UseSslConfig] = "maybe"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_IgnoresInvalidDateValues()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.SinceConfig] = "not-a-date"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_AcceptsCompleteConfig()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.PortConfig] = "993",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.PasswordConfig] = "password123",
            [ImapConnectorConfig.UseSslConfig] = "true",
            [ImapConnectorConfig.TimeoutSecondsConfig] = "30",
            [ImapConnectorConfig.AcceptInvalidCertificatesConfig] = "false",
            [ImapConnectorConfig.FolderConfig] = "INBOX",
            [ImapConnectorConfig.BatchSizeConfig] = "100",
            [ImapConnectorConfig.UseIdleConfig] = "true",
            [ImapConnectorConfig.MarkAsReadConfig] = "false",
            [ImapConnectorConfig.DeleteAfterReadConfig] = "false",
            [ImapConnectorConfig.StartFromConfig] = "latest",
            [ImapConnectorConfig.UnseenOnlyConfig] = "true",
            [ImapConnectorConfig.IncludeBodyConfig] = "true",
            [ImapConnectorConfig.IncludeAttachmentsConfig] = "false",
            [ImapConnectorConfig.MaxAttachmentSizeBytesConfig] = "10485760",
            [ImapConnectorConfig.PreferHtmlConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_AfterStart_CleansUpResources()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        task.Start(config);

        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimesAfterStart()
    {
        using var task = new ImapSourceTask();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Stop();
            task.Stop();
        });

        Assert.Null(exception);
    }
}
