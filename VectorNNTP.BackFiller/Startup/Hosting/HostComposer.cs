// <copyright file="HostComposer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Startup.Hosting
// Composes the worker host and registers the runtime services used during startup and shutdown.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Runtime.Listener;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.Backfiller.Startup.Hosting
{
    /// <summary>
    /// Composes the validated runtime host by wiring logging, DI registrations, shutdown policy, and hosted-service startup graph.
    /// </summary>
    /// <remarks>
    /// This type is the composition boundary between startup validation output and runtime execution: it receives a
    /// validated immutable runtime snapshot, registers runtime services, and builds the <see cref="IHost"/> instance
    /// that <see cref="HostLifetimeCoordinator"/> later runs.
    /// </remarks>
    internal static class HostComposer
    {
        /// <summary>
        /// Registers <see cref="TimeProvider.System"/> as the single application time source.
        /// </summary>
        /// <param name="services">Service collection that receives the time-provider singleton registration.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterTimeProvider(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);

            _ = services.AddSingleton(TimeProvider.System);
        }

        /// <summary>
        /// Configures the Generic Host shutdown timeout from BackFiller shutdown options.
        /// </summary>
        /// <param name="services">Service collection to configure with host options.</param>
        /// <param name="shutdownOptions">BackFiller shutdown configuration containing grace-period settings.</param>
        /// <remarks>
        /// <para>Sets the Generic Host's <see cref="HostOptions.ShutdownTimeout"/> to the configured value.
        /// This timeout uses the same shared shutdown budget consumed by worker drain policy
        /// (<c>BackFiller:Shutdown:GracePeriodSeconds</c>).</para>
        /// <para>The Generic Host timer and the <see cref="ShutdownCoordinator"/> grace-period timer are
        /// independent mechanisms that may start from different lifecycle events. This service intentionally
        /// aligns them to the same configured duration while relying on the external supervisor timeout as the
        /// larger final safety boundary.</para>
        ///
        /// <para>If this timeout is exceeded:</para>
        /// <list type="bullet">
        /// <item><description>The shutdown cancellation token is signalled and hosted services are expected to terminate promptly.</description></item>
        /// <item><description>Non-cooperative operations may continue beyond this deadline and must ultimately be bounded by the external process supervisor.</description></item>
        /// <item><description>The Generic Host may continue awaiting hosted-service shutdown tasks after cancellation has been requested; cooperative cancellation is therefore required for shutdown to complete promptly.</description></item>
        /// </list>
        ///
        /// <para><b>Configuration guidance:</b> Avoid arbitrary production values. Choose
        /// <c>BackFiller:Shutdown:GracePeriodSeconds</c> from the maximum intentional in-flight work that must drain
        /// (for example active article operations, RabbitMQ in-flight work, channel leases, TransitServer streams,
        /// queue depth allowed during shutdown, and downstream throughput limits).</para>
        /// <para>Then set the external supervisor timeout (for example systemd <c>TimeoutStopSec</c>) substantially higher
        /// to provide a hard-stop process termination deadline, with the invariant <c>TimeoutStopSec &gt; HostOptions.ShutdownTimeout</c>.</para>
        /// <para><b>Invariant:</b> <paramref name="shutdownOptions"/> is expected to be pre-validated during startup
        /// configuration validation. This method also applies a defensive range guard because it can be called independently.</para>
        /// </remarks>
        internal static void ConfigureHostShutdownTimeout(
            IServiceCollection services,
            ShutdownOptions shutdownOptions)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(shutdownOptions);

            if (shutdownOptions.GracePeriodSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(shutdownOptions),
                    shutdownOptions.GracePeriodSeconds,
                    "ShutdownOptions.GracePeriodSeconds must be greater than zero.");
            }

            TimeSpan shutdownTimeout = TimeSpan.FromSeconds(shutdownOptions.GracePeriodSeconds);

            _ = services.Configure<HostOptions>(options =>
                options.ShutdownTimeout = shutdownTimeout);
        }

        /// <summary>
        /// Registers the authoritative lifecycle state machine used across startup and runtime transitions.
        /// </summary>
        /// <param name="services">Service collection that receives lifecycle registration.</param>
        /// <param name="lifecycle">Optional pre-created lifecycle instance; when <see langword="null"/>, a new singleton is registered.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterServiceLifecycle(IServiceCollection services, ServiceLifecycle? lifecycle)
        {
            ArgumentNullException.ThrowIfNull(services);

            _ = lifecycle is null
                ? services.AddSingleton<ServiceLifecycle>()
                : services.AddSingleton(lifecycle);
        }

        /// <summary>
        /// Registers shutdown-coordination infrastructure for graceful-to-forced cancellation flow.
        /// </summary>
        /// <param name="services">Service collection that receives the shutdown-coordinator singleton.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterShutdownCoordinator(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<ShutdownCoordinator>();
        }

        /// <summary>
        /// Registers runtime NNTP account snapshot services and startup initialization entry point.
        /// </summary>
        /// <param name="services">Service collection that receives account snapshot runtime registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterRuntimeAccountSnapshotServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<MySqlNntpAccountSnapshotProvider>();
            _ = services.AddHostedService<NntpAccountSnapshotStartupInitializer>();
        }

        /// <summary>
        /// Registers RabbitMQ connectivity, topology initialization, and hosted consumer runtime services.
        /// </summary>
        /// <param name="services">Service collection that receives RabbitMQ infrastructure registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterRabbitMqInfrastructureServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<RabbitMqConnectionManager>();
            _ = services.AddSingleton<RabbitMqTopologyInitializer>();
            _ = services.AddSingleton<IRabbitMqConsumerSessionFactory, RabbitMqConsumerSessionFactory>();
            _ = services.AddHostedService<RabbitMqStartupInitializer>();
            _ = services.AddSingleton<RabbitMqConsumerService>();
            _ = services.AddSingleton<IRabbitMqCapacityRetirementCoordinator>(static provider => provider.GetRequiredService<RabbitMqConsumerService>());
            _ = services.AddHostedService(static provider => provider.GetRequiredService<RabbitMqConsumerService>());
        }

        /// <summary>
        /// Registers transit publishing runtime components and their startup initializer.
        /// </summary>
        /// <param name="services">Service collection that receives transit publisher registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterTransitPublisherServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<TransitPublisher>();
            _ = services.AddHostedService<TransitPublisherStartupInitializer>();
        }

        /// <summary>
        /// Registers certificate provisioning state/services, listener startup dependency, and renewal hosted services.
        /// </summary>
        /// <param name="services">Service collection that receives certificate and listener-related registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterCertificateServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<BackFillerCertificateState>();
            _ = services.AddSingleton<BackFillerCertificateStore>();
            _ = services.AddSingleton<IAuthoritativeDnsTxtPropagationVerifier, AuthoritativeDnsTxtPropagationVerifier>();
            _ = services.AddSingleton<IAcmeCertificateIssuer, AcmeCertificateIssuer>();
            _ = services.AddSingleton<BackFillerCertificateProvisioningService>();
            _ = services.AddHostedService<BackFillerCertificateStartupInitializer>();
            _ = services.AddHostedService<BackFillerListenerSocketService>();
            _ = services.AddHostedService<LetsEncryptCertificateRenewalService>();
        }

        /// <summary>
        /// Registers control-plane state/services and hosted runtime loop dependencies.
        /// </summary>
        /// <param name="services">Service collection that receives control-plane registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterControlPlaneService(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<BackboneUsableCapacityState>();
            _ = services.AddSingleton<IBackboneUsableCapacityProvider>(static provider => provider.GetRequiredService<BackboneUsableCapacityState>());
            _ = services.AddSingleton<IBackboneUsableCapacityStateWriter>(static provider => provider.GetRequiredService<BackboneUsableCapacityState>());
            _ = services.AddSingleton<ControlPlaneService>();
            _ = services.AddSingleton<IBackboneSessionLeaseProvider>(static provider => provider.GetRequiredService<ControlPlaneService>());
            _ = services.AddHostedService(static provider => provider.GetRequiredService<ControlPlaneService>());
        }

        /// <summary>
        /// Registers article-processing workflow services that consume RabbitMQ work and publish ARTICLE outcomes.
        /// </summary>
        /// <param name="services">Service collection that receives article-processing registrations.</param>
        /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
        internal static void RegisterArticleProcessingServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<NntpArticleGrabberWorkflow>();
            _ = services.AddSingleton<IRabbitMqArticleWorkRequestParser, RabbitMqArticleWorkRequestParser>();
            _ = services.AddSingleton<IBackboneArticleRetriever, BackboneArticleRetriever>();
            _ = services.AddSingleton<IArticleWorkProcessor, ArticleWorkProcessor>();
            _ = services.AddSingleton<IArticleWorkDispositionPlanner, ArticleWorkDispositionPlanner>();
            _ = services.AddSingleton<IArticleWorkResponseFactory, ArticleWorkResponseFactory>();
            _ = services.AddSingleton<IRabbitMqArticleResponsePublisher, RabbitMqArticleResponsePublisher>();
            _ = services.AddSingleton<IArticleWorkResultSink, RabbitMqArticleResultSink>();
            _ = services.AddHostedService<RabbitMqArticleProcessingService>();
        }

        /// <summary>
        /// Formats the validated Generic Host shutdown timeout description used by startup diagnostics.
        /// </summary>
        /// <param name="shutdownOptions">Validated shutdown options containing the grace-period value.</param>
        /// <returns>Formatted text describing Generic Host shutdown timeout in seconds.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="shutdownOptions"/> is <see langword="null"/>.</exception>
        internal static string GetHostShutdownTimeoutDescription(ShutdownOptions shutdownOptions)
        {
            ArgumentNullException.ThrowIfNull(shutdownOptions);
            return $"Generic Host shutdown timeout: {shutdownOptions.GracePeriodSeconds}s";
        }

        /// <summary>
        /// Applies runtime service registrations and host options using validated startup runtime configuration.
        /// </summary>
        /// <param name="hostBuilder">Host application builder whose service collection is populated.</param>
        /// <param name="runtimeOptions">Validated immutable runtime options snapshot produced by startup validation.</param>
        /// <param name="lifecycle">Optional pre-created authoritative lifecycle instance to preserve across startup/runtime.</param>
        /// <exception cref="ArgumentNullException"><paramref name="hostBuilder"/> or <paramref name="runtimeOptions"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// Registration order documents intended startup dependencies, but runtime readiness ordering is enforced by
        /// hosted-service behavior and startup orchestration rather than DI registration order alone.
        /// </remarks>
        internal static void ConfigureHostServices(
            HostApplicationBuilder hostBuilder,
            BackFillerRuntimeOptions runtimeOptions,
            ServiceLifecycle? lifecycle = null)
        {
            ArgumentNullException.ThrowIfNull(hostBuilder);
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            IServiceCollection services = hostBuilder.Services;

            ShutdownOptions shutdownOptions = new()
            {
                GracePeriodSeconds = runtimeOptions.ShutdownGracePeriodSeconds,
                DrainQueuedWork = runtimeOptions.ShutdownDrainQueuedWork,
                FinishActiveArticles = runtimeOptions.ShutdownFinishActiveArticles,
            };

            ConfigureHostShutdownTimeout(services, shutdownOptions);

            // Register time provider for UTC-based timing throughout the application.
            // Enables deterministic time in tests and consistent UTC handling for timeouts, retries, metrics.
            RegisterTimeProvider(services);

            // Register the authoritative application lifecycle instance created during startup so
            // validation/build transitions and runtime readiness/shutdown transitions share one state machine.
            RegisterServiceLifecycle(services, lifecycle);

            // Register shutdown coordination service for graceful->forced cancellation flow.
            RegisterShutdownCoordinator(services);

            // Register startup-time NNTP account snapshot loading before runtime loops start.
            RegisterRuntimeAccountSnapshotServices(services);

            // Register RabbitMQ startup initialization after account load so topology can be scoped per backbone.
            RegisterRabbitMqInfrastructureServices(services);

            // Register Phase 3 RabbitMQ article processing/classification services.
            RegisterArticleProcessingServices(services);

            // Register transit publisher startup initialization before control-plane runtime loops start.
            RegisterTransitPublisherServices(services);

            // Register ACME/TLS certificate lifecycle services and periodic renewal loop.
            RegisterCertificateServices(services);

            // Phase 6: Register the control-plane hosted service.
            // Runtime readiness dependencies must be established through explicit service dependencies
            // and startup orchestration, not IServiceCollection registration order.
            RegisterControlPlaneService(services);
        }

        /// <summary>
        /// Configures production logging, registers validated runtime services, and builds the executable host instance.
        /// </summary>
        /// <param name="builder">Host application builder with merged configuration sources.</param>
        /// <param name="runtimeOptions">Validated runtime options snapshot registered for downstream service consumption.</param>
        /// <param name="serviceLifecycle">Authoritative lifecycle instance preserved through host composition and runtime execution.</param>
        /// <returns>The composed <see cref="IHost"/> instance ready for <see cref="HostLifetimeCoordinator.RunAsync(IHost, ServiceLifecycle, Action)"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder"/>, <paramref name="runtimeOptions"/>, or <paramref name="serviceLifecycle"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// This method also logs a non-secret configuration fingerprint after logging pipeline configuration and before
        /// host build to aid deployment diagnostics.
        /// </remarks>
        internal static IHost ComposeHost(
            HostApplicationBuilder builder,
            BackFillerRuntimeOptions runtimeOptions,
            ServiceLifecycle serviceLifecycle)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(serviceLifecycle);

            Logging.SerilogConfigurator.ConfigureSerilogLogging(
                builder.Services,
                builder.Configuration,
                "VectorNNTP.Backfiller",
                runtimeOptions.ValidatedLogDirectory);

            _ = builder.Services.AddSingleton(runtimeOptions);

            BuildInfoService.LogConfigurationFingerprint(builder.Configuration);
            ConfigureHostServices(builder, runtimeOptions, serviceLifecycle);

            return builder.Build();
        }
    }
}
