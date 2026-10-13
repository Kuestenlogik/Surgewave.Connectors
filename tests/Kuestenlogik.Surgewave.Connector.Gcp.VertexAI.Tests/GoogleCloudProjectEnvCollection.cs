namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI.Tests;

using Xunit;

/// <summary>
/// xunit collection for tests that read and write the <c>GOOGLE_CLOUD_PROJECT</c>
/// environment variable. It is process-global, so these tests need serialised
/// execution — otherwise the tests see each other's values. A race with the
/// symptom "Failed: 1, Passed: 35" fired flakily in the release workflow,
/// not in the CI workflow (differently scheduled thread-pool layout).
/// Classes with [Collection(Name)] therefore run sequentially.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class GoogleCloudProjectEnvCollection
{
    public const string Name = "GoogleCloudProjectEnv";
}
