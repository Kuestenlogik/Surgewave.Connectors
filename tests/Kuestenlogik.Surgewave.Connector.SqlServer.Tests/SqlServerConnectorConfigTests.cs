using Xunit;
using Kuestenlogik.Surgewave.Connector.SqlServer;

namespace Kuestenlogik.Surgewave.Connector.SqlServer.Tests;

public class SqlServerConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("sqlserver.connection.string", SqlServerConnectorConfig.ConnectionString);
        Assert.Equal("sqlserver.server", SqlServerConnectorConfig.Server);
        Assert.Equal("sqlserver.database", SqlServerConnectorConfig.Database);
        Assert.Equal("sqlserver.username", SqlServerConnectorConfig.Username);
        Assert.Equal("sqlserver.password", SqlServerConnectorConfig.Password);
        Assert.Equal("sqlserver.trust.server.certificate", SqlServerConnectorConfig.TrustServerCertificate);
        Assert.Equal("sqlserver.encrypt", SqlServerConnectorConfig.Encrypt);
    }

    [Fact]
    public void CdcSettings_HaveExpectedValues()
    {
        Assert.Equal("sqlserver.tables", SqlServerConnectorConfig.Tables);
        Assert.Equal("sqlserver.topic.prefix", SqlServerConnectorConfig.TopicPrefix);
        Assert.Equal("sqlserver.topic.pattern", SqlServerConnectorConfig.TopicPattern);
        Assert.Equal("sqlserver.include.schema", SqlServerConnectorConfig.IncludeSchema);
        Assert.Equal("sqlserver.include.before.values", SqlServerConnectorConfig.IncludeBeforeValues);
    }

    [Fact]
    public void SnapshotSettings_HaveExpectedValues()
    {
        Assert.Equal("sqlserver.snapshot.mode", SqlServerConnectorConfig.SnapshotMode);
    }

    [Fact]
    public void PollingSettings_HaveExpectedValues()
    {
        Assert.Equal("sqlserver.poll.interval.ms", SqlServerConnectorConfig.PollIntervalMs);
        Assert.Equal("sqlserver.batch.max.records", SqlServerConnectorConfig.BatchMaxRecords);
    }

    [Fact]
    public void CdcTrackingSettings_HaveExpectedValues()
    {
        Assert.Equal("sqlserver.capture.instance", SqlServerConnectorConfig.CaptureInstance);
        Assert.Equal("sqlserver.start.from.beginning", SqlServerConnectorConfig.StartFromBeginning);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("lsn", SqlServerConnectorConfig.OffsetLsn);
        Assert.Equal("seqval", SqlServerConnectorConfig.OffsetSeqVal);
        Assert.Equal("snapshot.completed", SqlServerConnectorConfig.OffsetSnapshotCompleted);
        Assert.Equal("timestamp", SqlServerConnectorConfig.OffsetTimestamp);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("localhost", SqlServerConnectorConfig.DefaultServer);
        Assert.Equal(1433, SqlServerConnectorConfig.DefaultPort);
        Assert.Equal("${schema}.${table}", SqlServerConnectorConfig.DefaultTopicPattern);
        Assert.Equal(500L, SqlServerConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(1000, SqlServerConnectorConfig.DefaultBatchMaxRecords);
    }

    [Fact]
    public void SnapshotModes_HaveExpectedValues()
    {
        Assert.Equal("initial", SqlServerConnectorConfig.SnapshotModeInitial);
        Assert.Equal("never", SqlServerConnectorConfig.SnapshotModeNever);
        Assert.Equal("always", SqlServerConnectorConfig.SnapshotModeAlways);
        Assert.Equal("schema_only", SqlServerConnectorConfig.SnapshotModeSchemaOnly);
    }

    [Fact]
    public void CdcOperationTypes_HaveExpectedValues()
    {
        Assert.Equal(1, SqlServerConnectorConfig.CdcOperationDelete);
        Assert.Equal(2, SqlServerConnectorConfig.CdcOperationInsert);
        Assert.Equal(3, SqlServerConnectorConfig.CdcOperationUpdateBefore);
        Assert.Equal(4, SqlServerConnectorConfig.CdcOperationUpdateAfter);
    }
}
