using Kuestenlogik.Surgewave.Connector.Imap;

namespace Kuestenlogik.Surgewave.Connector.Imap.Tests;

public class ImapConnectorConfigTests
{
    [Fact]
    public void TopicConfig_HasExpectedValue()
    {
        Assert.Equal("topic", ImapConnectorConfig.TopicConfig);
    }

    [Fact]
    public void HostConfig_HasExpectedValue()
    {
        Assert.Equal("imap.host", ImapConnectorConfig.HostConfig);
    }

    [Fact]
    public void PortConfig_HasExpectedValue()
    {
        Assert.Equal("imap.port", ImapConnectorConfig.PortConfig);
    }

    [Fact]
    public void UsernameConfig_HasExpectedValue()
    {
        Assert.Equal("imap.username", ImapConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void PasswordConfig_HasExpectedValue()
    {
        Assert.Equal("imap.password", ImapConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void UseSslConfig_HasExpectedValue()
    {
        Assert.Equal("imap.use.ssl", ImapConnectorConfig.UseSslConfig);
    }

    [Fact]
    public void TimeoutSecondsConfig_HasExpectedValue()
    {
        Assert.Equal("imap.timeout.seconds", ImapConnectorConfig.TimeoutSecondsConfig);
    }

    [Fact]
    public void AcceptInvalidCertificatesConfig_HasExpectedValue()
    {
        Assert.Equal("imap.accept.invalid.certificates", ImapConnectorConfig.AcceptInvalidCertificatesConfig);
    }

    [Fact]
    public void FolderConfig_HasExpectedValue()
    {
        Assert.Equal("imap.folder", ImapConnectorConfig.FolderConfig);
    }

    [Fact]
    public void FoldersConfig_HasExpectedValue()
    {
        Assert.Equal("imap.folders", ImapConnectorConfig.FoldersConfig);
    }

    [Fact]
    public void RecursiveConfig_HasExpectedValue()
    {
        Assert.Equal("imap.recursive", ImapConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("imap.poll.interval.ms", ImapConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void UseIdleConfig_HasExpectedValue()
    {
        Assert.Equal("imap.use.idle", ImapConnectorConfig.UseIdleConfig);
    }

    [Fact]
    public void IdleTimeoutMinutesConfig_HasExpectedValue()
    {
        Assert.Equal("imap.idle.timeout.minutes", ImapConnectorConfig.IdleTimeoutMinutesConfig);
    }

    [Fact]
    public void BatchSizeConfig_HasExpectedValue()
    {
        Assert.Equal("imap.batch.size", ImapConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void MarkAsReadConfig_HasExpectedValue()
    {
        Assert.Equal("imap.mark.as.read", ImapConnectorConfig.MarkAsReadConfig);
    }

    [Fact]
    public void DeleteAfterReadConfig_HasExpectedValue()
    {
        Assert.Equal("imap.delete.after.read", ImapConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void MoveAfterReadConfig_HasExpectedValue()
    {
        Assert.Equal("imap.move.after.read", ImapConnectorConfig.MoveAfterReadConfig);
    }

    [Fact]
    public void MoveToFolderConfig_HasExpectedValue()
    {
        Assert.Equal("imap.move.to.folder", ImapConnectorConfig.MoveToFolderConfig);
    }

    [Fact]
    public void StartFromConfig_HasExpectedValue()
    {
        Assert.Equal("imap.start.from", ImapConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void UnseenOnlyConfig_HasExpectedValue()
    {
        Assert.Equal("imap.unseen.only", ImapConnectorConfig.UnseenOnlyConfig);
    }

    [Fact]
    public void SinceConfig_HasExpectedValue()
    {
        Assert.Equal("imap.since", ImapConnectorConfig.SinceConfig);
    }

    [Fact]
    public void SubjectFilterConfig_HasExpectedValue()
    {
        Assert.Equal("imap.subject.filter", ImapConnectorConfig.SubjectFilterConfig);
    }

    [Fact]
    public void FromFilterConfig_HasExpectedValue()
    {
        Assert.Equal("imap.from.filter", ImapConnectorConfig.FromFilterConfig);
    }

    [Fact]
    public void IncludeBodyConfig_HasExpectedValue()
    {
        Assert.Equal("imap.include.body", ImapConnectorConfig.IncludeBodyConfig);
    }

    [Fact]
    public void IncludeAttachmentsConfig_HasExpectedValue()
    {
        Assert.Equal("imap.include.attachments", ImapConnectorConfig.IncludeAttachmentsConfig);
    }

    [Fact]
    public void MaxAttachmentSizeBytesConfig_HasExpectedValue()
    {
        Assert.Equal("imap.max.attachment.size.bytes", ImapConnectorConfig.MaxAttachmentSizeBytesConfig);
    }

    [Fact]
    public void PreferHtmlConfig_HasExpectedValue()
    {
        Assert.Equal("imap.prefer.html", ImapConnectorConfig.PreferHtmlConfig);
    }

    // Default value tests
    [Fact]
    public void DefaultPort_Is993()
    {
        Assert.Equal(993, ImapConnectorConfig.DefaultPort);
    }

    [Fact]
    public void DefaultPortNoSsl_Is143()
    {
        Assert.Equal(143, ImapConnectorConfig.DefaultPortNoSsl);
    }

    [Fact]
    public void DefaultTimeoutSeconds_Is30()
    {
        Assert.Equal(30, ImapConnectorConfig.DefaultTimeoutSeconds);
    }

    [Fact]
    public void DefaultUseSsl_IsTrue()
    {
        Assert.True(ImapConnectorConfig.DefaultUseSsl);
    }

    [Fact]
    public void DefaultFolder_IsInbox()
    {
        Assert.Equal("INBOX", ImapConnectorConfig.DefaultFolder);
    }

    [Fact]
    public void DefaultPollIntervalMs_Is30000()
    {
        Assert.Equal(30000, ImapConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultUseIdle_IsTrue()
    {
        Assert.True(ImapConnectorConfig.DefaultUseIdle);
    }

    [Fact]
    public void DefaultIdleTimeoutMinutes_Is29()
    {
        Assert.Equal(29, ImapConnectorConfig.DefaultIdleTimeoutMinutes);
    }

    [Fact]
    public void DefaultBatchSize_Is100()
    {
        Assert.Equal(100, ImapConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultMarkAsRead_IsFalse()
    {
        Assert.False(ImapConnectorConfig.DefaultMarkAsRead);
    }

    [Fact]
    public void DefaultDeleteAfterRead_IsFalse()
    {
        Assert.False(ImapConnectorConfig.DefaultDeleteAfterRead);
    }

    [Fact]
    public void DefaultUnseenOnly_IsTrue()
    {
        Assert.True(ImapConnectorConfig.DefaultUnseenOnly);
    }

    [Fact]
    public void DefaultIncludeBody_IsTrue()
    {
        Assert.True(ImapConnectorConfig.DefaultIncludeBody);
    }

    [Fact]
    public void DefaultIncludeAttachments_IsFalse()
    {
        Assert.False(ImapConnectorConfig.DefaultIncludeAttachments);
    }

    [Fact]
    public void DefaultMaxAttachmentSizeBytes_Is10MB()
    {
        Assert.Equal(10 * 1024 * 1024, ImapConnectorConfig.DefaultMaxAttachmentSizeBytes);
    }

    [Fact]
    public void DefaultPreferHtml_IsFalse()
    {
        Assert.False(ImapConnectorConfig.DefaultPreferHtml);
    }

    [Fact]
    public void DefaultStartFrom_IsLatest()
    {
        Assert.Equal("latest", ImapConnectorConfig.DefaultStartFrom);
    }

    [Fact]
    public void StartFromLatest_HasExpectedValue()
    {
        Assert.Equal("latest", ImapConnectorConfig.StartFromLatest);
    }

    [Fact]
    public void StartFromEarliest_HasExpectedValue()
    {
        Assert.Equal("earliest", ImapConnectorConfig.StartFromEarliest);
    }

    [Fact]
    public void OffsetUid_HasExpectedValue()
    {
        Assert.Equal("uid", ImapConnectorConfig.OffsetUid);
    }

    [Fact]
    public void OffsetFolder_HasExpectedValue()
    {
        Assert.Equal("folder", ImapConnectorConfig.OffsetFolder);
    }
}
