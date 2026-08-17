namespace VectorNNTP.BackFiller.Benchmarks;

internal static class FormatHelpers
{
    internal static void PrintRequiredRateComparison(double currentArticlesPerSecond, int articleBytes)
    {
        double bitsPerArticle = articleBytes * 8d;

        double articlesPerSecond10Gbps = 10_000_000_000d / bitsPerArticle;
        double articlesPerSecond20Gbps = 20_000_000_000d / bitsPerArticle;
        double articlesPerSecond30Gbps = 30_000_000_000d / bitsPerArticle;
        double articlesPerSecond40Gbps = 40_000_000_000d / bitsPerArticle;

        double requiredImprovementFor10Gbps = currentArticlesPerSecond <= 0
            ? double.PositiveInfinity
            : articlesPerSecond10Gbps / currentArticlesPerSecond;

        double currentTo10GbpsRatio = articlesPerSecond10Gbps <= 0
            ? 0
            : currentArticlesPerSecond / articlesPerSecond10Gbps;

        Console.WriteLine($"Required rate for 10 Gbps: {articlesPerSecond10Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 20 Gbps: {articlesPerSecond20Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 30 Gbps: {articlesPerSecond30Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 40 Gbps: {articlesPerSecond40Gbps:F4} articles/sec");
        Console.WriteLine($"Current rate: {currentArticlesPerSecond:F4} articles/sec");
        Console.WriteLine($"Required improvement for 10 Gbps: {requiredImprovementFor10Gbps:F4}x");
        Console.WriteLine($"Current rate / 10 Gbps target ratio: {currentTo10GbpsRatio:F4}");
    }

    internal static string BuildDepthBucketSummary(List<long>[] submitBuckets, List<long>[] completeBuckets)
    {
        string[] labels = ["1-4", "5-8", "9-12", "13-16", ">16"];
        List<string> parts = new(capacity: labels.Length);
        for (int i = 0; i < labels.Length; i++)
        {
            double submitAvg = submitBuckets[i].Count == 0 ? 0 : MetricMathHelpers.TicksToUs(submitBuckets[i].Average());
            double submitP95 = MetricMathHelpers.PercentileUs(submitBuckets[i], 0.95);
            double completeAvg = completeBuckets[i].Count == 0 ? 0 : MetricMathHelpers.TicksToUs(completeBuckets[i].Average());
            double completeP95 = MetricMathHelpers.PercentileUs(completeBuckets[i], 0.95);
            parts.Add($"Depth {labels[i]}: submit avg={submitAvg:F2}us p95={submitP95:F2}us | complete avg={completeAvg:F2}us p95={completeP95:F2}us");
        }

        return string.Join("; ", parts);
    }
}
