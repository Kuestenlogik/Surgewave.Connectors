using Kuestenlogik.Surgewave.Connector.Mirror.Filters;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Filters;

public class ConsumerGroupFilterTests
{
    [Fact]
    public void ShouldSync_WithMatchAllPattern_ShouldMatchAll()
    {
        var filter = new ConsumerGroupFilter(".*", [], []);

        Assert.True(filter.ShouldSync("my-app"));
        Assert.True(filter.ShouldSync("consumer-group-1"));
        Assert.True(filter.ShouldSync("test.group"));
    }

    [Fact]
    public void ShouldSync_WithSpecificPattern_ShouldOnlyMatchPattern()
    {
        var filter = new ConsumerGroupFilter("my-app.*", [], []);

        Assert.True(filter.ShouldSync("my-app"));
        Assert.True(filter.ShouldSync("my-app-1"));
        Assert.True(filter.ShouldSync("my-app.worker"));
        Assert.False(filter.ShouldSync("other-app"));
    }

    [Fact]
    public void ShouldSync_WithWhitelist_ShouldOnlyMatchWhitelist()
    {
        var filter = new ConsumerGroupFilter(".*", ["app-1", "app-2"], []);

        Assert.True(filter.ShouldSync("app-1"));
        Assert.True(filter.ShouldSync("app-2"));
        Assert.False(filter.ShouldSync("app-3"));
    }

    [Fact]
    public void ShouldSync_WithBlacklist_ShouldExcludeBlacklisted()
    {
        var filter = new ConsumerGroupFilter(".*", [], ["test-group", "internal-group"]);

        Assert.True(filter.ShouldSync("my-app"));
        Assert.True(filter.ShouldSync("production-consumer"));
        Assert.False(filter.ShouldSync("test-group"));
        Assert.False(filter.ShouldSync("internal-group"));
    }

    [Fact]
    public void ShouldSync_BlacklistTakesPrecedenceOverWhitelist()
    {
        var filter = new ConsumerGroupFilter(".*", ["app-1", "test-app"], ["test-app"]);

        Assert.True(filter.ShouldSync("app-1"));
        Assert.False(filter.ShouldSync("test-app"));
    }

    [Fact]
    public void FilterGroups_ShouldReturnOnlyMatchingGroups()
    {
        var filter = new ConsumerGroupFilter("prod-.*", [], []);
        var groups = new[] { "prod-app-1", "prod-app-2", "dev-app", "test-app" };

        var result = filter.FilterGroups(groups).ToList();

        Assert.Equal(2, result.Count);
        Assert.Contains("prod-app-1", result);
        Assert.Contains("prod-app-2", result);
    }

    [Fact]
    public void FilterGroups_ShouldExcludeBlacklistedGroups()
    {
        var filter = new ConsumerGroupFilter(".*", [], ["test", "internal"]);
        var groups = new[] { "app-1", "test", "app-2", "internal" };

        var result = filter.FilterGroups(groups).ToList();

        Assert.Equal(2, result.Count);
        Assert.Contains("app-1", result);
        Assert.Contains("app-2", result);
    }

    [Theory]
    [InlineData("^app-\\d+$", "app-1", true)]
    [InlineData("^app-\\d+$", "app-123", true)]
    [InlineData("^app-\\d+$", "app-abc", false)]
    [InlineData("^(prod|staging)-.*", "prod-service", true)]
    [InlineData("^(prod|staging)-.*", "staging-service", true)]
    [InlineData("^(prod|staging)-.*", "dev-service", false)]
    public void ShouldSync_ShouldSupportComplexRegexPatterns(string pattern, string group, bool expected)
    {
        var filter = new ConsumerGroupFilter(pattern, [], []);
        Assert.Equal(expected, filter.ShouldSync(group));
    }
}
