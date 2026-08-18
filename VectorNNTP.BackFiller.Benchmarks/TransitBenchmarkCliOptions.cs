namespace VectorNNTP.BackFiller.Benchmarks;

internal readonly record struct TransitBenchmarkCliOptions(
    int? DurationSeconds,
    int? WarmupSeconds,
    int? ConnectionPoolSize,
    int? PipelineDepth,
    int? DispatchWorkers,
    int? QueueMegabytes,
    int? QueueArticles,
    int? ArticleKilobytes,
    int? GeneratorWorkers,
    int? WriteBatchCoalesceMicroseconds,
    int? ArticleCount = null,
    string? ExpectedAssemblyPath = null,
    string? ExpectedAssemblyVersion = null,
    string? ExpectedFileVersion = null,
    string? ExpectedConfiguration = null,
    string? ExpectedPlatform = null,
    string? ExpectedTargetFramework = null,
    string? ExpectedRuntimeIdentifier = null,
    string? ExpectedArchitecture = null,
    string? ExpectedProductionAssemblyPath = null,
    string? ExpectedProductionAssemblyVersion = null,
    string? ExpectedProductionFileVersion = null)
{
    internal static TransitBenchmarkCliOptions Parse(string[] args)
    {
        if (args.Length == 0)
        {
            return default;
        }

        int? durationSeconds = null;
        int? warmupSeconds = null;
        int? connectionPoolSize = null;
        int? pipelineDepth = null;
        int? dispatchWorkers = null;
        int? queueMegabytes = null;
        int? queueArticles = null;
        int? articleKilobytes = null;
        int? generatorWorkers = null;
        int? writeBatchCoalesceMicroseconds = null;
        int? articleCount = null;
        string? expectedAssemblyPath = null;
        string? expectedAssemblyVersion = null;
        string? expectedFileVersion = null;
        string? expectedConfiguration = null;
        string? expectedPlatform = null;
        string? expectedTargetFramework = null;
        string? expectedRuntimeIdentifier = null;
        string? expectedArchitecture = null;
        string? expectedProductionAssemblyPath = null;
        string? expectedProductionAssemblyVersion = null;
        string? expectedProductionFileVersion = null;

        for (int i = 0; i < args.Length; i++)
        {
            string token = args[i];
            if (!token.StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Unrecognized argument '{token}'. Expected --key value format.");
            }

            string key;
            string value;

            int equalsIndex = token.IndexOf('=');
            if (equalsIndex > 2)
            {
                key = token[2..equalsIndex];
                value = token[(equalsIndex + 1)..];
            }
            else
            {
                key = token[2..];
                if (i + 1 >= args.Length)
                {
                    throw new ArgumentException($"Missing value for option '{token}'.");
                }

                value = args[++i];
            }

            switch (key.ToLowerInvariant())
            {
                case "duration-seconds":
                    durationSeconds = ParsePositiveInt(key, value);
                    break;
                case "warmup-seconds":
                    warmupSeconds = ParsePositiveInt(key, value);
                    break;
                case "connections":
                    connectionPoolSize = ParsePositiveInt(key, value);
                    break;
                case "pipeline-depth":
                    pipelineDepth = ParsePositiveInt(key, value);
                    break;
                case "dispatch-workers":
                    dispatchWorkers = ParsePositiveInt(key, value);
                    break;
                case "queue-mib":
                    queueMegabytes = ParsePositiveInt(key, value);
                    break;
                case "queue-articles":
                    queueArticles = ParsePositiveInt(key, value);
                    break;
                case "article-kib":
                    articleKilobytes = ParsePositiveInt(key, value);
                    break;
                case "generator-workers":
                    generatorWorkers = ParsePositiveInt(key, value);
                    break;
                case "write-batch-coalesce-us":
                    writeBatchCoalesceMicroseconds = ParsePositiveInt(key, value);
                    break;
                case "article-count":
                    articleCount = ParsePositiveInt(key, value);
                    break;
                case "expected-assembly-path":
                    expectedAssemblyPath = ParseRequiredString(key, value);
                    break;
                case "expected-assembly-version":
                    expectedAssemblyVersion = ParseRequiredString(key, value);
                    break;
                case "expected-file-version":
                    expectedFileVersion = ParseRequiredString(key, value);
                    break;
                case "expected-configuration":
                    expectedConfiguration = ParseRequiredString(key, value);
                    break;
                case "expected-platform":
                    expectedPlatform = ParseRequiredString(key, value);
                    break;
                case "expected-target-framework":
                    expectedTargetFramework = ParseRequiredString(key, value);
                    break;
                case "expected-runtime-identifier":
                    expectedRuntimeIdentifier = ParseRequiredString(key, value);
                    break;
                case "expected-architecture":
                    expectedArchitecture = ParseRequiredString(key, value);
                    break;
                case "expected-production-assembly-path":
                    expectedProductionAssemblyPath = ParseRequiredString(key, value);
                    break;
                case "expected-production-assembly-version":
                    expectedProductionAssemblyVersion = ParseRequiredString(key, value);
                    break;
                case "expected-production-file-version":
                    expectedProductionFileVersion = ParseRequiredString(key, value);
                    break;
                default:
                    throw new ArgumentException($"Unknown option '--{key}'.");
            }
        }

        return new TransitBenchmarkCliOptions(
            DurationSeconds: durationSeconds,
            WarmupSeconds: warmupSeconds,
            ConnectionPoolSize: connectionPoolSize,
            PipelineDepth: pipelineDepth,
            DispatchWorkers: dispatchWorkers,
            QueueMegabytes: queueMegabytes,
            QueueArticles: queueArticles,
            ArticleKilobytes: articleKilobytes,
            GeneratorWorkers: generatorWorkers,
            WriteBatchCoalesceMicroseconds: writeBatchCoalesceMicroseconds,
            ArticleCount: articleCount,
            ExpectedAssemblyPath: expectedAssemblyPath,
            ExpectedAssemblyVersion: expectedAssemblyVersion,
            ExpectedFileVersion: expectedFileVersion,
            ExpectedConfiguration: expectedConfiguration,
            ExpectedPlatform: expectedPlatform,
            ExpectedTargetFramework: expectedTargetFramework,
            ExpectedRuntimeIdentifier: expectedRuntimeIdentifier,
            ExpectedArchitecture: expectedArchitecture,
            ExpectedProductionAssemblyPath: expectedProductionAssemblyPath,
            ExpectedProductionAssemblyVersion: expectedProductionAssemblyVersion,
            ExpectedProductionFileVersion: expectedProductionFileVersion);
    }

    private static int ParsePositiveInt(string key, string raw)
    {
        if (!int.TryParse(raw, out int parsed) || parsed <= 0)
        {
            throw new ArgumentException($"Option '--{key}' requires a positive integer value. Received '{raw}'.");
        }

        return parsed;
    }

    private static string ParseRequiredString(string key, string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            throw new ArgumentException($"Option '--{key}' requires a non-empty value.");
        }

        return raw;
    }
}
