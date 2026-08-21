using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SpaCy.Tests;

/// <summary>
/// Tests for the NLP document <see cref="SpaCySinkTask"/> builds out of a spaCy response.
/// The configured operations decide which views a downstream consumer gets, and asking for
/// nothing but NER must not drag every token along.
/// </summary>
public class SpaCyOutputTests
{
    private const string SpaCyResponse = """
        {
          "tokens": [
            {"text":"Berlin","pos":"PROPN","tag":"NNP","lemma":"Berlin","dep":"nsubj","head":2},
            {"text":"is","pos":"AUX","tag":"VBZ","lemma":"be","dep":"ROOT","head":2}
          ],
          "ents": [{"text":"Berlin","label":"GPE","start":0,"end":6}],
          "sents": [{"text":"Berlin is a city","start":0,"end":16}],
          "noun_chunks": [{"text":"a city","root":"city","start":10,"end":16}],
          "vectors": [0.25, 0.5]
        }
        """;

    [Fact]
    public void BuildOutput_WithOnlyNerRequested_LeavesTheTokenViewsOut()
    {
        using var output = Build("ner", includeVectors: false);

        Assert.True(output.RootElement.TryGetProperty("entities", out _));

        // Every token view multiplies the payload; a pipeline that only wants entities
        // should not pay for tokens, tags, lemmas and dependencies as well.
        Assert.False(output.RootElement.TryGetProperty("tokens", out _));
        Assert.False(output.RootElement.TryGetProperty("pos_tags", out _));
        Assert.False(output.RootElement.TryGetProperty("lemmas", out _));
        Assert.False(output.RootElement.TryGetProperty("dependencies", out _));
    }

    [Fact]
    public void BuildOutput_MapsEntitiesWithTheirLabelAndCharacterOffsets()
    {
        using var output = Build("ner", includeVectors: false);

        var entity = output.RootElement.GetProperty("entities")[0];

        // The offsets are what lets a consumer highlight the entity in the original text,
        // so they travel with every entity.
        Assert.Equal("Berlin", entity.GetProperty("text").GetString());
        Assert.Equal("GPE", entity.GetProperty("label").GetString());
        Assert.Equal(0, entity.GetProperty("start").GetInt32());
        Assert.Equal(6, entity.GetProperty("end").GetInt32());
    }

    [Fact]
    public void BuildOutput_ProjectsEachTokenOperationIntoItsOwnView()
    {
        using var output = Build("tokenize,pos,lemma,dep", includeVectors: false);

        var tokens = output.RootElement.GetProperty("tokens");
        Assert.Equal(2, tokens.GetArrayLength());
        Assert.Equal("Berlin", tokens[0].GetString());

        Assert.Equal("PROPN", output.RootElement.GetProperty("pos_tags")[0].GetProperty("pos").GetString());
        Assert.Equal("NNP", output.RootElement.GetProperty("pos_tags")[0].GetProperty("tag").GetString());
        Assert.Equal("be", output.RootElement.GetProperty("lemmas")[1].GetProperty("lemma").GetString());
        Assert.Equal("nsubj", output.RootElement.GetProperty("dependencies")[0].GetProperty("dep").GetString());
        Assert.Equal(2, output.RootElement.GetProperty("dependencies")[0].GetProperty("head").GetInt32());
    }

    [Fact]
    public void BuildOutput_AlwaysCarriesSentencesAndNounChunksTheServerReported()
    {
        using var output = Build("tokenize", includeVectors: false);

        // These two are not tied to an operation: whatever the server sends is passed on.
        Assert.Equal("Berlin is a city", output.RootElement.GetProperty("sentences")[0].GetProperty("text").GetString());
        Assert.Equal("city", output.RootElement.GetProperty("noun_chunks")[0].GetProperty("root").GetString());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BuildOutput_IncludesTheWordVectorsOnlyWhenAsked(bool includeVectors)
    {
        using var output = Build("tokenize", includeVectors);

        // Vectors are by far the largest part of a spaCy response, which is why they are
        // opt-in rather than always attached.
        Assert.Equal(includeVectors, output.RootElement.TryGetProperty("vectors", out _));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void BuildOutput_EchoesTheOriginalTextOnlyWhenAsked(string includeText, bool expected)
    {
        var config = SinkConfig();
        config[SpaCyConnectorConfig.Operations] = "ner";
        config[SpaCyConnectorConfig.IncludeText] = includeText;

        using var output = Build(config);

        Assert.Equal(expected, output.RootElement.TryGetProperty("text", out _));
    }

    private static JsonDocument Build(string operations, bool includeVectors)
    {
        var config = SinkConfig();
        config[SpaCyConnectorConfig.Operations] = operations;
        config[SpaCyConnectorConfig.IncludeVectors] = includeVectors ? "true" : "false";
        return Build(config);
    }

    private static JsonDocument Build(IDictionary<string, string> config)
    {
        using var task = new SpaCySinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var response = JsonDocument.Parse(SpaCyResponse);
        var output = task.BuildOutput("Berlin is a city", response.RootElement);

        return JsonDocument.Parse(JsonSerializer.Serialize(output));
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [SpaCyConnectorConfig.Topics] = "documents",
        [SpaCyConnectorConfig.OutputTopic] = "documents-nlp",
        [SpaCyConnectorConfig.ServerUrl] = "http://spacy.invalid:8080"
    };
}
