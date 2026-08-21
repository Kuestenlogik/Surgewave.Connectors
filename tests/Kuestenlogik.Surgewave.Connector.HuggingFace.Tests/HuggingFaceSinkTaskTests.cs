namespace Kuestenlogik.Surgewave.Connector.HuggingFace.Tests;

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class HuggingFaceSinkTaskTests
{
    [Fact]
    public void HuggingFaceSinkTask_HasCorrectVersion()
    {
        using var task = new HuggingFaceSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void HuggingFaceSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void HuggingFaceSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task HuggingFaceSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", HuggingFaceConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", HuggingFaceConnectorConfig.ModeConfig);
        Assert.Equal("sentiment", HuggingFaceConnectorConfig.ModeSentiment);
        Assert.Equal("ner", HuggingFaceConnectorConfig.ModeNer);
        Assert.Equal("classification", HuggingFaceConnectorConfig.ModeClassification);
        Assert.Equal("embeddings", HuggingFaceConnectorConfig.ModeEmbeddings);
        Assert.Equal("text-generation", HuggingFaceConnectorConfig.ModeTextGeneration);
        Assert.Equal("fill-mask", HuggingFaceConnectorConfig.ModeFillMask);
        Assert.Equal("question-answering", HuggingFaceConnectorConfig.ModeQuestionAnswering);
        Assert.Equal("summarization", HuggingFaceConnectorConfig.ModeSummarization);
        Assert.Equal("translation", HuggingFaceConnectorConfig.ModeTranslation);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectConnectionConfig()
    {
        Assert.Equal("huggingface.api.key", HuggingFaceConnectorConfig.ApiKeyConfig);
        Assert.Equal("huggingface.model.id", HuggingFaceConnectorConfig.ModelIdConfig);
        Assert.Equal("huggingface.endpoint", HuggingFaceConnectorConfig.EndpointConfig);
        Assert.Equal("https://api-inference.huggingface.co/models", HuggingFaceConnectorConfig.DefaultEndpoint);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", HuggingFaceConnectorConfig.InputFieldConfig);
        Assert.Equal("text", HuggingFaceConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", HuggingFaceConnectorConfig.OutputFieldConfig);
        Assert.Equal("result", HuggingFaceConnectorConfig.DefaultOutputField);
        Assert.Equal("embeddings.field", HuggingFaceConnectorConfig.EmbeddingsFieldConfig);
        Assert.Equal("embedding", HuggingFaceConnectorConfig.DefaultEmbeddingsField);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectQuestionAnsweringConfig()
    {
        Assert.Equal("context.field", HuggingFaceConnectorConfig.ContextFieldConfig);
        Assert.Equal("context", HuggingFaceConnectorConfig.DefaultContextField);
        Assert.Equal("question.field", HuggingFaceConnectorConfig.QuestionFieldConfig);
        Assert.Equal("question", HuggingFaceConnectorConfig.DefaultQuestionField);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectClassificationConfig()
    {
        Assert.Equal("candidate.labels", HuggingFaceConnectorConfig.CandidateLabelsConfig);
        Assert.Equal("multi.label", HuggingFaceConnectorConfig.MultiLabelConfig);
        Assert.False(HuggingFaceConnectorConfig.DefaultMultiLabel);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectTextGenerationConfig()
    {
        Assert.Equal("max.new.tokens", HuggingFaceConnectorConfig.MaxNewTokensConfig);
        Assert.Equal(50, HuggingFaceConnectorConfig.DefaultMaxNewTokens);
        Assert.Equal("temperature", HuggingFaceConnectorConfig.TemperatureConfig);
        Assert.Equal(1.0, HuggingFaceConnectorConfig.DefaultTemperature);
        Assert.Equal("top.k", HuggingFaceConnectorConfig.TopKConfig);
        Assert.Equal(50, HuggingFaceConnectorConfig.DefaultTopK);
        Assert.Equal("top.p", HuggingFaceConnectorConfig.TopPConfig);
        Assert.Equal(0.95, HuggingFaceConnectorConfig.DefaultTopP);
        Assert.Equal("do.sample", HuggingFaceConnectorConfig.DoSampleConfig);
        Assert.True(HuggingFaceConnectorConfig.DefaultDoSample);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, HuggingFaceConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, HuggingFaceConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, HuggingFaceConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, HuggingFaceConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(HuggingFaceConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", HuggingFaceConnectorConfig.FormatJson);
        Assert.Equal("merge", HuggingFaceConnectorConfig.FormatMerge);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectDefaultModels()
    {
        Assert.Contains("distilbert", HuggingFaceConnectorConfig.DefaultSentimentModel);
        Assert.Contains("bert", HuggingFaceConnectorConfig.DefaultNerModel);
        Assert.Contains("bart", HuggingFaceConnectorConfig.DefaultClassificationModel);
        Assert.Contains("sentence-transformers", HuggingFaceConnectorConfig.DefaultEmbeddingsModel);
        Assert.Contains("gpt2", HuggingFaceConnectorConfig.DefaultTextGenerationModel);
        Assert.Contains("bert", HuggingFaceConnectorConfig.DefaultFillMaskModel);
        Assert.Contains("roberta", HuggingFaceConnectorConfig.DefaultQuestionAnsweringModel);
        Assert.Contains("bart", HuggingFaceConnectorConfig.DefaultSummarizationModel);
        Assert.Contains("Helsinki-NLP", HuggingFaceConnectorConfig.DefaultTranslationModel);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectWaitForModelConfig()
    {
        Assert.Equal("wait.for.model", HuggingFaceConnectorConfig.WaitForModelConfig);
        Assert.True(HuggingFaceConnectorConfig.DefaultWaitForModel);
    }

    [Fact]
    public async Task HuggingFaceSinkTask_FlushAsync_DrainsBufferedRecordsToWebhook()
    {
        var port = 26200 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        var webhookHits = 0;
        var serverTask = Task.Run(() => RunMockServerAsync(listener, webhookStatusCode: 200, onWebhookHit: () => Interlocked.Increment(ref webhookHits)));

        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());
        task.Start(CreateMockServerConfig(port));

        try
        {
            // Below batch size: PutAsync only buffers, nothing is inferred yet
            await task.PutAsync([CreateRecord()]);
            Assert.Equal(0, Volatile.Read(ref webhookHits));

            // FlushAsync must drain the buffer - the worker commits consumer offsets
            // right after it, so anything left in the buffer would be lost on restart
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
            Assert.Equal(1, Volatile.Read(ref webhookHits));
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }

    [Fact]
    public async Task HuggingFaceSinkTask_FlushAsync_ThrowsWhenWebhookDeliveryFails()
    {
        var port = 26400 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        var serverTask = Task.Run(() => RunMockServerAsync(listener, webhookStatusCode: 500, onWebhookHit: () => { }));

        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());
        task.Start(CreateMockServerConfig(port));

        try
        {
            await task.PutAsync([CreateRecord()]);

            // A failing webhook means the result was NOT delivered - the flush must
            // throw so the worker retries/DLQs instead of committing the offset
            await Assert.ThrowsAsync<HttpRequestException>(
                () => task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None));
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }

    [Fact]
    public async Task HuggingFaceSinkTask_TranslationMode_SendsConfiguredLanguagePair()
    {
        var port = 26600 + (Environment.ProcessId % 200);
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();

        var inferenceBodies = new ConcurrentQueue<string>();
        var serverTask = Task.Run(() => RunMockServerAsync(listener, webhookStatusCode: 200, onWebhookHit: () => { }, inferenceBodies));

        var config = CreateMockServerConfig(port);
        config[HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeTranslation;
        config[HuggingFaceConnectorConfig.SourceLanguageConfig] = "eng_Latn";
        config[HuggingFaceConnectorConfig.TargetLanguageConfig] = "deu_Latn";

        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());
        task.Start(config);

        try
        {
            await task.PutAsync([CreateRecord()]);
            await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

            Assert.True(inferenceBodies.TryDequeue(out var body));
            Assert.Contains("\"src_lang\":\"eng_Latn\"", body, StringComparison.Ordinal);
            Assert.Contains("\"tgt_lang\":\"deu_Latn\"", body, StringComparison.Ordinal);
        }
        finally
        {
            task.Stop();
            listener.Stop();
            await serverTask;
        }
    }

    private static Dictionary<string, string> CreateMockServerConfig(int port)
    {
        return new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ApiKeyConfig] = "test-api-key",
            [HuggingFaceConnectorConfig.ModelIdConfig] = "test-model",
            [HuggingFaceConnectorConfig.EndpointConfig] = $"http://localhost:{port}/models",
            [HuggingFaceConnectorConfig.WebhookUrlConfig] = $"http://localhost:{port}/webhook",
            [HuggingFaceConnectorConfig.BatchSizeConfig] = "100",
            [HuggingFaceConnectorConfig.BatchTimeoutMsConfig] = "600000",
            [HuggingFaceConnectorConfig.RetryMaxConfig] = "0",
            [HuggingFaceConnectorConfig.RetryBackoffMsConfig] = "1",
            [HuggingFaceConnectorConfig.WaitForModelConfig] = "false"
        };
    }

    private static SinkRecord CreateRecord()
    {
        return new SinkRecord
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes("{\"text\":\"hello\"}"),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private static async Task RunMockServerAsync(
        HttpListener listener,
        int webhookStatusCode,
        Action onWebhookHit,
        ConcurrentQueue<string>? inferenceBodies = null)
    {
        const string inferenceJson = """[{ "label": "POSITIVE", "score": 0.99 }]""";

        try
        {
            while (listener.IsListening)
            {
                var ctx = await listener.GetContextAsync();

                if (ctx.Request.Url!.AbsolutePath.EndsWith("/webhook", StringComparison.Ordinal))
                {
                    onWebhookHit();
                    ctx.Response.StatusCode = webhookStatusCode;
                }
                else
                {
                    if (inferenceBodies != null)
                    {
                        using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                        inferenceBodies.Enqueue(await reader.ReadToEndAsync());
                    }

                    var body = Encoding.UTF8.GetBytes(inferenceJson);
                    ctx.Response.ContentType = "application/json";
                    ctx.Response.StatusCode = 200;
                    await ctx.Response.OutputStream.WriteAsync(body);
                }

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
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
