using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.GraphQL;

namespace Kuestenlogik.Surgewave.Connector.GraphQL.Tests;

public class GraphQLSourceTaskOffsetTests
{
    [Fact]
    public async Task PollAsync_RecordOffsets_CarryTheRecordsOwnCursor()
    {
        var port = 27180 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/graphql/");
        listener.Start();

        var serverTask = Task.Run(async () =>
        {
            try
            {
                while (listener.IsListening)
                {
                    var ctx = await listener.GetContextAsync();
                    var body = Encoding.UTF8.GetBytes(
                        "{\"data\":{\"items\":[" +
                        "{\"id\":\"1\",\"ts\":\"2026-01-01T00:00:00Z\"}," +
                        "{\"id\":\"2\",\"ts\":\"2026-01-02T00:00:00Z\"}]}}");
                    ctx.Response.ContentType = "application/json";
                    ctx.Response.StatusCode = 200;
                    await ctx.Response.OutputStream.WriteAsync(body);
                    ctx.Response.Close();
                }
            }
            catch (HttpListenerException)
            {
                // Listener stopped
            }
            catch (ObjectDisposedException)
            {
                // Listener disposed
            }
        });

        using var task = new GraphQLSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = $"http://localhost:{port}/graphql/",
            [GraphQLConnectorConfig.TopicConfig] = "graphql-data",
            [GraphQLConnectorConfig.QueryConfig] = "query { items { id ts } }",
            [GraphQLConnectorConfig.DataPathConfig] = "items",
            [GraphQLConnectorConfig.IdFieldConfig] = "id",
            [GraphQLConnectorConfig.TimestampFieldConfig] = "ts",
            [GraphQLConnectorConfig.PollIntervalMsConfig] = "0"
        });

        try
        {
            var records = await task.PollAsync(CancellationToken.None);

            // Each record's SourceOffset must carry that record's own cursor, not the
            // previous record's - otherwise a restart re-fetches the last committed record.
            Assert.Equal(2, records.Count);
            Assert.Equal("1", records[0].SourceOffset[GraphQLConnectorConfig.OffsetLastId]);
            Assert.Equal("2026-01-01T00:00:00Z", records[0].SourceOffset[GraphQLConnectorConfig.OffsetLastTimestamp]);
            Assert.Equal("2", records[1].SourceOffset[GraphQLConnectorConfig.OffsetLastId]);
            Assert.Equal("2026-01-02T00:00:00Z", records[1].SourceOffset[GraphQLConnectorConfig.OffsetLastTimestamp]);
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }
}
