using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Spark.Tests;

/// <summary>
/// Tests for how the Spark sink turns a record into a job command. Everything the connector
/// does afterwards - which endpoint it calls, how much memory the driver gets - is decided
/// here, from a record that an arbitrary producer wrote.
/// </summary>
public class SparkSinkTaskTests : IDisposable
{
    private readonly List<SparkSinkTask> _tasks = [];

    [Fact]
    public void ParseCommand_WithAnEmptyValue_FallsBackToTheConfiguredAction()
    {
        var task = StartTask(SinkConfig(SparkConnectorConfig.ActionTypeCreateSession));

        var command = task.ParseCommand(Record(Array.Empty<byte>()));

        // A tombstone carries no instruction, so the connector's own default decides -
        // guessing "submit" would launch a job nobody asked for.
        Assert.Equal(SparkConnectorConfig.ActionTypeCreateSession, command.Action);
    }

    [Fact]
    public void ParseCommand_WithAValueThatIsNotACommand_FallsBackToTheConfiguredAction()
    {
        var task = StartTask(SinkConfig(SparkConnectorConfig.ActionTypeStatement));

        var command = task.ParseCommand(Record("this is not a command"));

        Assert.Equal(SparkConnectorConfig.ActionTypeStatement, command.Action);
    }

    [Fact]
    public void ParseCommand_FillsTheGapsWithTheConfiguredClusterDefaults()
    {
        var config = SinkConfig(SparkConnectorConfig.ActionTypeSubmit);
        config[SparkConnectorConfig.SessionKind] = SparkConnectorConfig.SessionKindPySpark;
        config[SparkConnectorConfig.DriverMemory] = "4g";
        config[SparkConnectorConfig.DriverCores] = "2";
        config[SparkConnectorConfig.ExecutorMemory] = "8g";
        config[SparkConnectorConfig.ExecutorCores] = "4";
        config[SparkConnectorConfig.NumExecutors] = "12";
        var task = StartTask(config);

        var command = task.ParseCommand(Record("""{"file":"s3://jobs/etl.py"}"""));

        // A producer that only names the file still gets the sizing the connector was
        // configured with, instead of Livy's much smaller built-in defaults.
        Assert.Equal(SparkConnectorConfig.ActionTypeSubmit, command.Action);
        Assert.Equal(SparkConnectorConfig.ApiModeLivy, command.ApiMode);
        Assert.Equal(SparkConnectorConfig.SessionKindPySpark, command.Kind);
        Assert.Equal("4g", command.DriverMemory);
        Assert.Equal(2, command.DriverCores);
        Assert.Equal("8g", command.ExecutorMemory);
        Assert.Equal(4, command.ExecutorCores);
        Assert.Equal(12, command.NumExecutors);
    }

    [Fact]
    public void ParseCommand_KeepsWhatTheRecordItselfSpecifies()
    {
        var config = SinkConfig(SparkConnectorConfig.ActionTypeSubmit);
        config[SparkConnectorConfig.DriverMemory] = "1g";
        var task = StartTask(config);

        var command = task.ParseCommand(Record(
            """{"action":"kill","apiMode":"spark","submissionId":"driver-42","driverMemory":"16g","numExecutors":32}"""));

        // Per-job overrides have to win over the connector defaults, otherwise a single
        // sink can only ever run one shape of job.
        Assert.Equal(SparkConnectorConfig.ActionTypeKill, command.Action);
        Assert.Equal(SparkConnectorConfig.ApiModeSpark, command.ApiMode);
        Assert.Equal("driver-42", command.SubmissionId);
        Assert.Equal("16g", command.DriverMemory);
        Assert.Equal(32, command.NumExecutors);
    }

    [Fact]
    public void ParseCommand_AcceptsWhateverCasingTheProducerUsed()
    {
        var task = StartTask(SinkConfig(SparkConnectorConfig.ActionTypeSubmit));

        var command = task.ParseCommand(Record("""{"Action":"statement","SESSIONID":3,"Code":"spark.version"}"""));

        Assert.Equal(SparkConnectorConfig.ActionTypeStatement, command.Action);
        Assert.Equal(3, command.SessionId);
        Assert.Equal("spark.version", command.Code);
    }

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            task.Stop();
            task.Dispose();
        }

        _tasks.Clear();
    }

    private SparkSinkTask StartTask(IDictionary<string, string> config)
    {
        var task = new SparkSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);
        _tasks.Add(task);
        return task;
    }

    private static Dictionary<string, string> SinkConfig(string action) => new()
    {
        [SparkConnectorConfig.Topics] = "spark-commands",
        [SparkConnectorConfig.LivyUrl] = "http://livy.invalid:8998",
        [SparkConnectorConfig.ActionType] = action
    };

    private static SinkRecord Record(string value) => Record(Encoding.UTF8.GetBytes(value));

    private static SinkRecord Record(byte[] value) => new()
    {
        Topic = "spark-commands",
        Partition = 0,
        Offset = 1,
        Value = value,
        Timestamp = DateTimeOffset.UnixEpoch
    };
}
