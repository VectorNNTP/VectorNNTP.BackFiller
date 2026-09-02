// <copyright file="TransitBenchmarkConfig.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Configuration/TransitBenchmarkConfig: binds and normalizes benchmark runtime settings.

using Microsoft.Extensions.Configuration;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the benchmark Mode enum used by the benchmark or regression gate.
/// </summary>
internal enum BenchmarkMode
{
    Validation,
    Full,
    Saturation,
    Forensic,
}

/// <summary>
/// Represents the transit BenchmarkConfig record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct TransitBenchmarkConfig(
    BenchmarkMode Mode,
    long BenchmarkInstanceId,
    string EndpointType,
    string EndpointIdentity,
    string EndpointHost,
    int EndpointPort,
    bool EndpointUseSsl,
    string AppSettingsPath,
    TimeSpan WarmupDuration,
    TimeSpan MeasurementDuration,
    int? MeasurementArticleCount,
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
    /// <summary>
    /// Gets or sets the required TransitHostname.
    /// </summary>
    private const string RequiredTransitHostname = "incoming.usenet.ninja";
    /// <summary>
    /// Gets or sets the default ArticleTargetBytes.
    /// </summary>
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    /// <summary>
    /// Gets or sets the default WarmupSeconds.
    /// </summary>
    private const int DefaultWarmupSeconds = 10;

    /// <summary>
    /// Loads and validates the benchmark configuration for the current appsettings file and any optional endpoint overrides.
    /// </summary>
    internal static TransitBenchmarkConfig Load(
            TimeSpan measurementDuration,
            BenchmarkMode mode,
            TransitBenchmarkCliOptions cliOptions,
            string? endpointHostOverride = null,
            int? endpointPortOverride = null,
            bool? endpointUseSslOverride = null,
            string endpointType = "TRANSITSERVER",
            string endpointIdentity = "appsettings:BackFiller:TransitServer")
    {
        string appSettingsPath = FindBackFillerAppSettingsPath();

        IConfigurationRoot configuration = new ConfigurationBuilder()
            .AddJsonFile(appSettingsPath, optional: false, reloadOnChange: false)
            .Build();

        return LoadFromConfiguration(
            measurementDuration,
            mode,
            cliOptions,
            configuration,
            appSettingsPath,
            endpointHostOverride,
            endpointPortOverride,
            endpointUseSslOverride,
            endpointType,
            endpointIdentity);
    }

    /// <summary>
    /// Builds and validates a transit benchmark configuration snapshot from an existing application configuration and optional overrides.
    /// </summary>
    /// <param name="measurementDuration">
    /// The duration to reserve for the benchmark measurement window. This value must be positive and is validated against the benchmark runtime constraints.
    /// </param>
    /// <param name="mode">
    /// The benchmark execution mode used to select the intended validation or load profile.
    /// </param>
    /// <param name="cliOptions">
    /// The CLI options that supply user-selected benchmark parameters and overrides.
    /// </param>
    /// <param name="configuration">
    /// The backing application configuration to read the configured TransitServer endpoint and benchmark defaults from.
    /// </param>
    /// <param name="appSettingsPath">
    /// The source appsettings path used to identify the configuration file and to preserve the original config source identity in the resulting snapshot.
    /// </param>
    /// <param name="endpointHostOverride">
    /// An optional host override for the endpoint used by the benchmark; when omitted, the host from configuration is used.
    /// </param>
    /// <param name="endpointPortOverride">
    /// An optional port override for the endpoint used by the benchmark; when omitted, the configured port is preserved.
    /// </param>
    /// <param name="endpointUseSslOverride">
    /// An optional SSL override for the endpoint used by the benchmark; when omitted, the configured SSL setting is preserved.
    /// </param>
    /// <param name="endpointType">
    /// The endpoint type identifier recorded in the benchmark snapshot.
    /// </param>
    /// <param name="endpointIdentity">
    /// The endpoint identity value recorded in the benchmark snapshot and used to disambiguate the reporting target.
    /// </param>
    /// <returns>
    /// A validated transit benchmark configuration snapshot containing the normalized benchmark parameters and the resolved endpoint identity.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="configuration"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="appSettingsPath"/>, <paramref name="endpointType"/>, or <paramref name="endpointIdentity"/> is empty or whitespace.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the appsettings file is missing required TransitServer settings, the resolved values fail validation, or the combined CLI and configuration settings exceed the supported benchmark bounds.
    /// </exception>
    internal static TransitBenchmarkConfig LoadFromConfiguration(
            TimeSpan measurementDuration,
            BenchmarkMode mode,
            TransitBenchmarkCliOptions cliOptions,
            IConfiguration configuration,
            string appSettingsPath,
            string? endpointHostOverride = null,
            int? endpointPortOverride = null,
            bool? endpointUseSslOverride = null,
            string endpointType = "TRANSITSERVER",
            string endpointIdentity = "appsettings:BackFiller:TransitServer")
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (string.IsNullOrWhiteSpace(appSettingsPath))
        {
            throw new ArgumentException("App settings path must be provided.", nameof(appSettingsPath));
        }

        if (cliOptions.ArticleCount is not null && cliOptions.DurationSeconds is not null)
        {
            throw new InvalidOperationException("Options '--article-count' and '--duration-seconds' are mutually exclusive for measurement execution.");
        }

        if (string.IsNullOrWhiteSpace(endpointType))
        {
            throw new ArgumentException("Endpoint type must be provided.", nameof(endpointType));
        }

        if (string.IsNullOrWhiteSpace(endpointIdentity))
        {
            throw new ArgumentException("Endpoint identity must be provided.", nameof(endpointIdentity));
        }

        string configuredHost = configuration["BackFiller:TransitServer:Host"]
            ?? throw new InvalidOperationException("BackFiller:TransitServer:Host is missing in existing application configuration.");

        string configuredNormalizedHost = configuredHost.Trim();
        if (!configuredNormalizedHost.Equals(RequiredTransitHostname, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Configured TransitServer host must be '{RequiredTransitHostname}', but was '{configuredNormalizedHost}'.");
        }

        string? configuredPortRaw = configuration["BackFiller:TransitServer:Port"];
        if (!int.TryParse(configuredPortRaw, out int configuredPort) || configuredPort is <= 0 or > 65535)
        {
            throw new InvalidOperationException("BackFiller:TransitServer:Port is missing or invalid in existing application configuration.");
        }

        bool configuredUseSsl = bool.TryParse(configuration["BackFiller:TransitServer:UseSsl"], out bool parsedUseSsl) && parsedUseSsl;

        string normalizedHost = endpointHostOverride?.Trim() ?? configuredNormalizedHost;
        if (string.IsNullOrWhiteSpace(normalizedHost))
        {
            throw new InvalidOperationException("Transit benchmark endpoint host resolved to empty value.");
        }

        int port = endpointPortOverride ?? configuredPort;
        if (port is <= 0 or > 65535)
        {
            throw new InvalidOperationException("Transit benchmark endpoint port is invalid.");
        }

        bool useSsl = endpointUseSslOverride ?? configuredUseSsl;

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

        int? measurementArticleCount = null;
        if (cliOptions.ArticleCount is int articleCount)
        {
            measurementArticleCount = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                articleCount,
                min: 1,
                max: 2_000_000,
                optionName: "article-count");
        }

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
            ExpectedArchitecture: cliOptions.ExpectedArchitecture,
            ExpectedProductionAssemblyPath: cliOptions.ExpectedProductionAssemblyPath,
            ExpectedProductionAssemblyVersion: cliOptions.ExpectedProductionAssemblyVersion,
            ExpectedProductionFileVersion: cliOptions.ExpectedProductionFileVersion);

        return new TransitBenchmarkConfig(
            Mode: mode,
            BenchmarkInstanceId: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            EndpointType: endpointType.Trim(),
            EndpointIdentity: endpointIdentity.Trim(),
            EndpointHost: normalizedHost,
            EndpointPort: port,
            EndpointUseSsl: useSsl,
            AppSettingsPath: appSettingsPath,
            WarmupDuration: TimeSpan.FromSeconds(warmupSeconds),
            MeasurementDuration: measurementDuration,
            MeasurementArticleCount: measurementArticleCount,
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

    /// <summary>
    /// Implements the find BackFillerAppSettingsPath contract.
    /// </summary>
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
