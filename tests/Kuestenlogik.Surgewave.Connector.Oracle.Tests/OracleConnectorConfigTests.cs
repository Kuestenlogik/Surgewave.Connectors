using Xunit;
using Kuestenlogik.Surgewave.Connector.Oracle;

namespace Kuestenlogik.Surgewave.Connector.Oracle.Tests;

public class OracleConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("oracle.connection.string", OracleConnectorConfig.ConnectionString);
        Assert.Equal("oracle.host", OracleConnectorConfig.Host);
        Assert.Equal("oracle.port", OracleConnectorConfig.Port);
        Assert.Equal("oracle.service.name", OracleConnectorConfig.ServiceName);
        Assert.Equal("oracle.sid", OracleConnectorConfig.Sid);
        Assert.Equal("oracle.username", OracleConnectorConfig.Username);
        Assert.Equal("oracle.password", OracleConnectorConfig.Password);
        Assert.Equal("oracle.wallet.location", OracleConnectorConfig.WalletLocation);
    }

    [Fact]
    public void CdcSettings_HaveExpectedValues()
    {
        Assert.Equal("oracle.tables", OracleConnectorConfig.Tables);
        Assert.Equal("oracle.topic.prefix", OracleConnectorConfig.TopicPrefix);
        Assert.Equal("oracle.topic.pattern", OracleConnectorConfig.TopicPattern);
        Assert.Equal("oracle.include.schema", OracleConnectorConfig.IncludeSchema);
        Assert.Equal("oracle.include.before.values", OracleConnectorConfig.IncludeBeforeValues);
    }

    [Fact]
    public void SnapshotSettings_HaveExpectedValues()
    {
        Assert.Equal("oracle.snapshot.mode", OracleConnectorConfig.SnapshotMode);
    }

    [Fact]
    public void PollingSettings_HaveExpectedValues()
    {
        Assert.Equal("oracle.poll.interval.ms", OracleConnectorConfig.PollIntervalMs);
        Assert.Equal("oracle.batch.max.records", OracleConnectorConfig.BatchMaxRecords);
    }

    [Fact]
    public void LogMinerSettings_HaveExpectedValues()
    {
        Assert.Equal("oracle.logminer.mode", OracleConnectorConfig.LogMinerMode);
        Assert.Equal("oracle.start.from.beginning", OracleConnectorConfig.StartFromBeginning);
        Assert.Equal("oracle.dictionary.mode", OracleConnectorConfig.DictionaryMode);
        Assert.Equal("oracle.supplemental.logging", OracleConnectorConfig.SupplementalLogging);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("scn", OracleConnectorConfig.OffsetScn);
        Assert.Equal("commit_scn", OracleConnectorConfig.OffsetCommitScn);
        Assert.Equal("snapshot.completed", OracleConnectorConfig.OffsetSnapshotCompleted);
        Assert.Equal("timestamp", OracleConnectorConfig.OffsetTimestamp);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("localhost", OracleConnectorConfig.DefaultHost);
        Assert.Equal(1521, OracleConnectorConfig.DefaultPort);
        Assert.Equal("${owner}.${table}", OracleConnectorConfig.DefaultTopicPattern);
        Assert.Equal(500L, OracleConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(1000, OracleConnectorConfig.DefaultBatchMaxRecords);
    }

    [Fact]
    public void SnapshotModes_HaveExpectedValues()
    {
        Assert.Equal("initial", OracleConnectorConfig.SnapshotModeInitial);
        Assert.Equal("never", OracleConnectorConfig.SnapshotModeNever);
        Assert.Equal("always", OracleConnectorConfig.SnapshotModeAlways);
        Assert.Equal("schema_only", OracleConnectorConfig.SnapshotModeSchemaOnly);
    }

    [Fact]
    public void LogMinerModes_HaveExpectedValues()
    {
        Assert.Equal("online", OracleConnectorConfig.LogMinerModeOnline);
        Assert.Equal("archived", OracleConnectorConfig.LogMinerModeArchived);
    }

    [Fact]
    public void DictionaryModes_HaveExpectedValues()
    {
        Assert.Equal("online", OracleConnectorConfig.DictionaryModeOnline);
        Assert.Equal("redo_log", OracleConnectorConfig.DictionaryModeRedoLog);
    }

    [Fact]
    public void OperationTypes_HaveExpectedValues()
    {
        Assert.Equal(1, OracleConnectorConfig.OperationInsert);
        Assert.Equal(2, OracleConnectorConfig.OperationDelete);
        Assert.Equal(3, OracleConnectorConfig.OperationUpdate);
        Assert.Equal(5, OracleConnectorConfig.OperationDdl);
        Assert.Equal(6, OracleConnectorConfig.OperationStart);
        Assert.Equal(7, OracleConnectorConfig.OperationCommit);
        Assert.Equal(36, OracleConnectorConfig.OperationRollback);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("oracle.owner", OracleConnectorConfig.HeaderSchema);
        Assert.Equal("oracle.table", OracleConnectorConfig.HeaderTable);
        Assert.Equal("oracle.op", OracleConnectorConfig.HeaderOperation);
        Assert.Equal("oracle.scn", OracleConnectorConfig.HeaderScn);
    }
}
