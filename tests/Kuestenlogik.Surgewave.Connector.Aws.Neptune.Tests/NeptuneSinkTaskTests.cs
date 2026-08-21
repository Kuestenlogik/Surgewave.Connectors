using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Neptune.Tests;

/// <summary>
/// Tests for <see cref="NeptuneSinkTask"/>. The Gremlin scripts are built and asserted directly,
/// so the escaping of labels, ids and property keys is verified without a Neptune cluster.
/// </summary>
public class NeptuneSinkTaskTests
{
    private static Dictionary<string, string> Config(string writeMode = "vertex", string vertexLabel = "person") =>
        new()
        {
            [NeptuneConnectorConfig.WriteMode] = writeMode,
            [NeptuneConnectorConfig.VertexLabel] = vertexLabel,
            [NeptuneConnectorConfig.EdgeLabel] = "knows",
            [NeptuneConnectorConfig.IdField] = "id",
            [NeptuneConnectorConfig.FromField] = "from",
            [NeptuneConnectorConfig.ToField] = "to"
        };

    // Mirrors how PutAsync parses a record: values arrive as JsonElement, not as CLR strings.
    private static Dictionary<string, object> Payload(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, object>>(json)!;

    private static SinkRecord CreateRecord(string json) =>
        new()
        {
            Topic = "graph",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes(json)
        };

    [Fact]
    public void BuildVertexQuery_AddsAVertexWithTheConfiguredLabelAndTheRecordId()
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config());

        var query = task.BuildVertexQuery(Payload("""{"id":"v1","name":"alpha"}"""));

        Assert.Equal("g.addV('person').property('id', 'v1').property('name', 'alpha')", query);
    }

    [Fact]
    public void BuildVertexQuery_EscapesQuotesInTheIdAndInPropertyValues()
    {
        // Unescaped input used to close the Gremlin string literal and inject traversal steps.
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config());

        var query = task.BuildVertexQuery(Payload("""{"id":"v'1","name":"O'Brien"}"""));

        Assert.Equal("""g.addV('person').property('id', 'v\'1').property('name', 'O\'Brien')""", query);
    }

    [Fact]
    public void BuildVertexQuery_EscapesBackslashesBeforeQuotes()
    {
        // Escaping quotes first would turn the backslash of the escaped quote into a new escape.
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config());

        var query = task.BuildVertexQuery(Payload("""{"id":"a\\b'c"}"""));

        Assert.Equal("""g.addV('person').property('id', 'a\\b\'c')""", query);
    }

    [Fact]
    public void BuildVertexQuery_EscapesTheLabelAndThePropertyKeys()
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config(vertexLabel: "per'son"));

        var query = task.BuildVertexQuery(Payload("""{"id":"v1","na'me":"alpha"}"""));

        Assert.Equal("""g.addV('per\'son').property('id', 'v1').property('na\'me', 'alpha')""", query);
    }

    [Fact]
    public void BuildVertexQuery_GeneratesAnIdWhenTheRecordCarriesNone()
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config());

        var query = task.BuildVertexQuery(Payload("""{"name":"alpha"}"""));

        const string prefix = "g.addV('person').property('id', '";
        Assert.StartsWith(prefix, query, StringComparison.Ordinal);
        Assert.True(Guid.TryParse(query[prefix.Length..].Split('\'')[0], out _));
        Assert.EndsWith(".property('name', 'alpha')", query, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildEdgeQuery_ConnectsTheConfiguredEndpointFields()
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config(writeMode: "edge"));

        var query = task.BuildEdgeQuery(Payload("""{"from":"v1","to":"v2","since":2026}"""));

        Assert.Equal("g.V('v1').addE('knows').to(g.V('v2')).property('since', '2026')", query);
    }

    [Fact]
    public void BuildEdgeQuery_ExcludesTheIdAndEndpointFieldsFromTheProperties()
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config(writeMode: "edge"));

        var query = task.BuildEdgeQuery(Payload("""{"id":"e1","from":"v1","to":"v2","weight":0.5}"""));

        Assert.Equal("g.V('v1').addE('knows').to(g.V('v2')).property('weight', '0.5')", query);
    }

    [Theory]
    [InlineData("""{"from":"v1"}""")]
    [InlineData("""{"to":"v2"}""")]
    [InlineData("""{"name":"alpha"}""")]
    public void BuildEdgeQuery_ReturnsNothingWhenAnEndpointIsMissing(string json)
    {
        using var task = new NeptuneSinkTask();
        task.ApplyConfiguration(Config(writeMode: "edge"));

        Assert.Null(task.BuildEdgeQuery(Payload(json)));
    }

    [Fact]
    public async Task PutAsync_FailsTheBatchOnAnUnparseablePayload()
    {
        // A dropped record used to be committed as if it had been written.
        var errors = new List<Exception>();
        using var task = new NeptuneSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfiguration(Config());

        await Assert.ThrowsAnyAsync<JsonException>(
            () => task.PutAsync([CreateRecord("not json")], CancellationToken.None));

        Assert.Single(errors);
    }

    [Fact]
    public async Task PutAsync_FailsTheBatchWhenTheWriteCannotBeSubmitted()
    {
        var errors = new List<Exception>();
        using var task = new NeptuneSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfiguration(Config());

        var thrown = await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync([CreateRecord("""{"id":"v1"}""")], CancellationToken.None));

        // The record is not acked behind a swallowed exception: the batch fails with it.
        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_SkipsAnEdgeRecordThatNamesNoEndpoints()
    {
        // Nothing is submitted, so the missing client is never touched.
        var errors = new List<Exception>();
        using var task = new NeptuneSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.ApplyConfiguration(Config(writeMode: "edge"));

        await task.PutAsync([CreateRecord("""{"from":"v1"}""")], CancellationToken.None);

        Assert.Empty(errors);
    }
}
