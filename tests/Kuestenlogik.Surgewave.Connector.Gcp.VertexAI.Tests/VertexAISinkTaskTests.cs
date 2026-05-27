namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

[Collection(GoogleCloudProjectEnvCollection.Name)]
public sealed class VertexAISinkTaskTests
{
    [Fact]
    public void VertexAISinkTask_HasCorrectVersion()
    {
        using var task = new VertexAISinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void VertexAISinkTask_Start_ThrowsOnMissingProjectId()
    {
        using var task = new VertexAISinkTask();
        task.Initialize(CreateTaskContext());

        // Clear environment variable if set
        var originalProjectId = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT");
        try
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", null);

            var config = new Dictionary<string, string>
            {
                [VertexAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
            Assert.Contains(VertexAIConnectorConfig.ProjectIdConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", originalProjectId);
        }
    }

    [Fact]
    public void VertexAISinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new VertexAISinkTask();
        task.Initialize(CreateTaskContext());

        // Don't start with real credentials - just test stop behavior
        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void VertexAISinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new VertexAISinkTask();
        task.Initialize(CreateTaskContext());

        // Don't start with real credentials - just test dispose behavior
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task VertexAISinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new VertexAISinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        // (buffer will be empty, no client to flush)
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectProjectIdConfig()
    {
        Assert.Equal("gcp.project.id", VertexAIConnectorConfig.ProjectIdConfig);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectLocationConfig()
    {
        Assert.Equal("gcp.location", VertexAIConnectorConfig.LocationConfig);
        Assert.Equal("us-central1", VertexAIConnectorConfig.DefaultLocation);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectCredentialsConfig()
    {
        Assert.Equal("gcp.credentials.json", VertexAIConnectorConfig.CredentialsJsonConfig);
        Assert.Equal("gcp.credentials.path", VertexAIConnectorConfig.CredentialsPathConfig);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", VertexAIConnectorConfig.ModeConfig);
        Assert.Equal("completions", VertexAIConnectorConfig.ModeCompletions);
        Assert.Equal("embeddings", VertexAIConnectorConfig.ModeEmbeddings);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectModelConfig()
    {
        Assert.Equal("model", VertexAIConnectorConfig.ModelConfig);
        Assert.Equal("gemini-2.0-flash", VertexAIConnectorConfig.DefaultModel);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectEmbeddingsConfig()
    {
        Assert.Equal("embeddings.model", VertexAIConnectorConfig.EmbeddingsModelConfig);
        Assert.Equal("text-embedding-005", VertexAIConnectorConfig.DefaultEmbeddingsModel);
        Assert.Equal("embeddings.dimensions", VertexAIConnectorConfig.EmbeddingsDimensionsConfig);
        Assert.Equal(768, VertexAIConnectorConfig.DefaultEmbeddingsDimensions);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectCompletionsDefaults()
    {
        Assert.Equal(1024, VertexAIConnectorConfig.DefaultMaxTokens);
        Assert.Equal(1.0, VertexAIConnectorConfig.DefaultTemperature);
        Assert.Equal(0.95, VertexAIConnectorConfig.DefaultTopP);
        Assert.Equal(40, VertexAIConnectorConfig.DefaultTopK);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, VertexAIConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, VertexAIConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, VertexAIConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, VertexAIConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectInputOutputDefaults()
    {
        Assert.Equal("text", VertexAIConnectorConfig.DefaultInputField);
        Assert.Equal("response", VertexAIConnectorConfig.DefaultOutputField);
        Assert.Equal("embedding", VertexAIConnectorConfig.DefaultEmbeddingsField);
        Assert.True(VertexAIConnectorConfig.DefaultIncludeOriginal);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectFormatConstants()
    {
        Assert.Equal("json", VertexAIConnectorConfig.FormatJson);
        Assert.Equal("merge", VertexAIConnectorConfig.FormatMerge);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
