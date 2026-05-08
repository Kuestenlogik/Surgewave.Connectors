using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Offsets;

public class OffsetTranslatorTests
{
    [Fact]
    public void StoreMapping_ShouldStoreOffset()
    {
        var translator = new OffsetTranslator();

        translator.StoreMapping("dc1", "orders", 0, 100, 95);

        var result = translator.Translate("dc1", "orders", 0, 100);
        Assert.Equal(95, result);
    }

    [Fact]
    public void Translate_ShouldReturnNullForUnknownMapping()
    {
        var translator = new OffsetTranslator();

        var result = translator.Translate("dc1", "orders", 0, 100);
        Assert.Null(result);
    }

    [Fact]
    public void Translate_ExactMatch_ShouldReturnExactValue()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 0, 200, 195);
        translator.StoreMapping("dc1", "orders", 0, 300, 295);

        var result = translator.Translate("dc1", "orders", 0, 200);
        Assert.Equal(195, result);
    }

    [Fact]
    public void Translate_Interpolation_ShouldEstimateBetweenKnownPoints()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 100);
        translator.StoreMapping("dc1", "orders", 0, 200, 200);

        // Request offset 150 (midpoint) should interpolate to ~150
        var result = translator.Translate("dc1", "orders", 0, 150);
        Assert.NotNull(result);
        Assert.Equal(150, result.Value);
    }

    [Fact]
    public void Translate_BeforeFirstMapping_ShouldReturnFirstValue()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 0, 200, 195);

        var result = translator.Translate("dc1", "orders", 0, 50);
        Assert.Equal(95, result);
    }

    [Fact]
    public void Translate_AfterLastMapping_ShouldExtrapolate()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 100);
        translator.StoreMapping("dc1", "orders", 0, 200, 200);

        // Request offset 250 (beyond last) should extrapolate: 200 + (250-200) = 250
        var result = translator.Translate("dc1", "orders", 0, 250);
        Assert.NotNull(result);
        Assert.Equal(250, result.Value);
    }

    [Fact]
    public void GetLatestTargetOffset_ShouldReturnMostRecentTarget()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 0, 200, 195);
        translator.StoreMapping("dc1", "orders", 0, 300, 295);

        var result = translator.GetLatestTargetOffset("dc1", "orders", 0);
        Assert.Equal(295, result);
    }

    [Fact]
    public void GetLatestSourceOffset_ShouldReturnMostRecentSource()
    {
        var translator = new OffsetTranslator();
        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 0, 200, 195);
        translator.StoreMapping("dc1", "orders", 0, 300, 295);

        var result = translator.GetLatestSourceOffset("dc1", "orders", 0);
        Assert.Equal(300, result);
    }

    [Fact]
    public void ShouldHandleMultipleTopicsAndPartitions()
    {
        var translator = new OffsetTranslator();

        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 1, 200, 195);
        translator.StoreMapping("dc1", "payments", 0, 300, 295);

        Assert.Equal(95, translator.Translate("dc1", "orders", 0, 100));
        Assert.Equal(195, translator.Translate("dc1", "orders", 1, 200));
        Assert.Equal(295, translator.Translate("dc1", "payments", 0, 300));
    }

    [Fact]
    public void ShouldHandleMultipleClusters()
    {
        var translator = new OffsetTranslator();

        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc2", "orders", 0, 100, 90);

        Assert.Equal(95, translator.Translate("dc1", "orders", 0, 100));
        Assert.Equal(90, translator.Translate("dc2", "orders", 0, 100));
    }

    [Fact]
    public void ShouldRespectMaxMappingsLimit()
    {
        var translator = new OffsetTranslator(maxMappingsPerPartition: 3);

        // Add 5 mappings - only last 3 should be kept
        translator.StoreMapping("dc1", "orders", 0, 100, 100);
        translator.StoreMapping("dc1", "orders", 0, 200, 200);
        translator.StoreMapping("dc1", "orders", 0, 300, 300);
        translator.StoreMapping("dc1", "orders", 0, 400, 400);
        translator.StoreMapping("dc1", "orders", 0, 500, 500);

        // First mapping should be evicted
        Assert.Equal(300, translator.Translate("dc1", "orders", 0, 100)); // Will use first available
        Assert.Equal(500, translator.GetLatestTargetOffset("dc1", "orders", 0));
    }

    [Fact]
    public void ShouldUpdateExistingMapping()
    {
        var translator = new OffsetTranslator();

        translator.StoreMapping("dc1", "orders", 0, 100, 95);
        translator.StoreMapping("dc1", "orders", 0, 100, 98); // Update same source offset

        var result = translator.Translate("dc1", "orders", 0, 100);
        Assert.Equal(98, result);
    }
}
