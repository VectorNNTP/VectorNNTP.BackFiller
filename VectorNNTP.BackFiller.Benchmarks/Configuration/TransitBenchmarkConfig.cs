using Microsoft.Extensions.Configuration;

namespace VectorNNTP.BackFiller.Benchmarks;

internal enum BenchmarkMode
{
    Validation,
    Full,
    Saturation,
    Forensic,
}

internal readonly record struct TransitBenchmarkConfig(
    BenchmarkMode Mode,
    long BenchmarkInstanceId,
    string EndpointHost,
    int EndpointPort,
    bool EndpointUseSsl,
    string AppSettingsPath,
    TimeSpan WarmupDuration,
    TimeSpan MeasurementDuration,
    int ConnectionPoolSize,
    int PerConnectionPipelineDepth,
    int DispatchWorkerCount,
    int GeneratorWorkerCount,
    int WriteBatchCoalesceMicroseconds,
    int MaxQueuedArticles,
    long MaxResidentBytes,
    int ArticleTargetBytes,
    int ProducerQueueTargetArticles,
    RuntimeIdentityExpectation ExpectedRuntimeIdentity)
{
    private const string RequiredTransitHostname = "incoming.usenet.ninja";
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    private const int DefaultWarmupSeconds = 10;

    internal static TransitBenchmarkConfig Load(TimeSpan measurementDuration, BenchmarkMode mode, TransitBenchmarkCliOptions cliOptions)
    {
        string appSettingsPath = FindBackFillerAppSettingsPath();

        IConfigurationRoot configuration = new ConfigurationBuilder()
            .AddJsonFile(appSettingsPath, optional: false, reloadOnChange: false)
            .Build();

        string host = configuration["BackFiller:TransitServer:Host"]
            ?? throw new InvalidOperationException("BackFiller:TransitServer:Host is missing in existing application configuration.");

        string normalizedHost = host.Trim();
        if (!normalizedHost.Equals(RequiredTransitHostname, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Configured TransitServer host must be '{RequiredTransitHostname}', but was '{normalizedHost}'.");
        }

        string? portRaw = configuration["BackFiller:TransitServer:Port"];
        if (!int.TryParse(portRaw, out int port) || port is <= 0 or > 65535)
        {
            throw new InvalidOperationException("BackFiller:TransitServer:Port is missing or invalid in existing application configuration.");
        }

        bool useSsl = bool.TryParse(configuration["BackFiller:TransitServer:UseSsl"], out bool parsedUseSsl) && parsedUseSsl;

        int connectionPoolSize = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.ConnectionPoolSize ?? 4, min: 1, max: 64, "connections");
        int perConnectionPipelineDepth = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.PipelineDepth ?? 8, min: 1, max: 64, "pipeline-depth");

        int defaultDispatchWorkers = checked(connectionPoolSize * perConnectionPipelineDepth);
        int dispatchWorkerCount = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.DispatchWorkers ?? defaultDispatchWorkers,
            min: 1,
            max: 512,
            optionName: "dispatch-workers");

        int articleTargetBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.ArticleKilobytes is null ? DefaultArticleTargetBytes : checked(cliOptions.ArticleKilobytes.Value * 1024),
            min: 128 * 1024,
            max: 4 * 1024 * 1024,
            optionName: "article-kib");

        int generatorWorkerCount = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.GeneratorWorkers ?? 1,
            min: 1,
            max: 512,
            optionName: "generator-workers");

        int writeBatchCoalesceMicroseconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.WriteBatchCoalesceMicroseconds ?? 250,
            min: 1,
            max: 50_000,
            optionName: "write-batch-coalesce-us");

        long maxResidentBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateLongRange(
            cliOptions.QueueMegabytes is null ? 256L * 1024L * 1024L : checked((long)cliOptions.QueueMegabytes.Value * 1024L * 1024L),
            min: 64L * 1024L * 1024L,
            max: 2L * 1024L * 1024L * 1024L,
            optionName: "queue-mib");

        int computedArticlesFromBytes = (int)Math.Max(1, maxResidentBytes / articleTargetBytes);
        int maxQueuedArticles = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.QueueArticles ?? Math.Max(64, computedArticlesFromBytes),
            min: 1,
            max: 200_000,
            optionName: "queue-articles");

        if (maxResidentBytes < articleTargetBytes)
        {
            throw new InvalidOperationException("Queue byte budget must be at least one article target size.");
        }

        int warmupSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.WarmupSeconds ?? DefaultWarmupSeconds, min: 1, max: 600, optionName: "warmup-seconds");

        int producerQueueTargetArticles = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            Math.Min(maxQueuedArticles, 2048),
            min: 1,
            max: maxQueuedArticles,
            optionName: "producer-queue-target-articles");

        RuntimeIdentityExpectation expectedRuntimeIdentity = new(
            ExpectedAssemblyPath: cliOptions.ExpectedAssemblyPath,
            ExpectedAssemblyVersion: cliOptions.ExpectedAssemblyVersion,
            ExpectedFileVersion: cliOptions.ExpectedFileVersion,
            ExpectedConfiguration: cliOptions.ExpectedConfiguration,
            ExpectedPlatform: cliOptions.ExpectedPlatform,
            ExpectedTargetFramework: cliOptions.ExpectedTargetFramework,
            ExpectedRuntimeIdentifier: cliOptions.ExpectedRuntimeIdentifier,
            ExpectedArchitecture: cliOptions.ExpectedArchitecture);

        return new TransitBenchmarkConfig(
            Mode: mode,
            BenchmarkInstanceId: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            EndpointHost: normalizedHost,
            EndpointPort: port,
            EndpointUseSsl: useSsl,
            AppSettingsPath: appSettingsPath,
            WarmupDuration: TimeSpan.FromSeconds(warmupSeconds),
            MeasurementDuration: measurementDuration,
            ConnectionPoolSize: connectionPoolSize,
            PerConnectionPipelineDepth: perConnectionPipelineDepth,
            DispatchWorkerCount: dispatchWorkerCount,
            GeneratorWorkerCount: generatorWorkerCount,
            WriteBatchCoalesceMicroseconds: writeBatchCoalesceMicroseconds,
            MaxQueuedArticles: maxQueuedArticles,
            MaxResidentBytes: maxResidentBytes,
            ArticleTargetBytes: articleTargetBytes,
            ProducerQueueTargetArticles: producerQueueTargetArticles,
            ExpectedRuntimeIdentity: expectedRuntimeIdentity);
    }

    private static string FindBackFillerAppSettingsPath()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);

        while (current is not null)
        {
            string candidate = Path.Combine(current.FullName, "VectorNNTP.BackFiller", "appsettings.json");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        throw new FileNotFoundException("Unable to locate existing BackFiller appsettings.json from benchmark runner base directory.");
    }
}
