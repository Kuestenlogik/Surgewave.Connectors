namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI.Tests;

using Xunit;

/// <summary>
/// xunit-Collection fuer Tests, die die <c>GOOGLE_CLOUD_PROJECT</c> Umgebungs-
/// variable lesen und schreiben. Process-global, deshalb braucht es eine
/// serialisierte Ausfuehrung — sonst sehen sich die Tests gegenseitig die
/// Werte. Race mit Symptom "Failed: 1, Passed: 35" hat im Release-Workflow
/// flakily gefeuert, im CI-Workflow nicht (anders gescheduletes Thread-Pool-
/// Layout). Klassen mit [Collection(Name)] gehen darauf hin sequentiell.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class GoogleCloudProjectEnvCollection
{
    public const string Name = "GoogleCloudProjectEnv";
}
