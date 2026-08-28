// <copyright file="HostComposer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Startup.Hosting
// Composes the worker host and registers the runtime services used during startup and shutdown.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Runtime.Listener;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.Backfiller.Startup.Hosting
{
    /// <summary>
    /// Owns production logging configuration, service registration, and host construction.
    /// </summary>
    /// <remarks>
    /// This type wires the startup-time lifecycle together, including account startup, certificate provisioning,
    /// the inbound TLS listener, control-plane services, shutdown coordination, and host timeout policy.
    /// </remarks>
    internal static class HostComposer
    {
        /// <summary>
        /// Registers <see cref="TimeProvider"/> as a singleton for unified time handling across the application.
        /// </summary>
        /// <param name="services">The service collection to register TimeProvider into.</param>
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
        /// Registers the authoritative application lifecycle instance for host composition and runtime transitions.
        /// </summary>
        /// <param name="services">Service collection to register the lifecycle into.</param>
        /// <param name="lifecycle">Optional authoritative lifecycle instance created before host build.</param>
        internal static void RegisterServiceLifecycle(IServiceCollection services, ServiceLifecycle? lifecycle)
        {
            ArgumentNullException.ThrowIfNull(services);

            _ = lifecycle is null
                ? services.AddSingleton<ServiceLifecycle>()
                : services.AddSingleton(lifecycle);
        }

        /// <summary>
        /// Registers the shutdown coordination service for graceful-to-forced cancellation flow.
        /// </summary>
        /// <param name="services">Service collection to register the shutdown coordinator into.</param>
        internal static void RegisterShutdownCoordinator(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<ShutdownCoordinator>();
        }

        /// <summary>
        /// Registers the runtime NNTP account snapshot provider and startup initializer.
        /// </summary>
        /// <param name="services">Service collection to register runtime account services into.</param>
        internal static void RegisterRuntimeAccountSnapshotServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<MySqlNntpAccountSnapshotProvider>();
            _ = services.AddHostedService<NntpAccountSnapshotStartupInitializer>();
        }

        /// <summary>
        /// Registers RabbitMQ infrastructure services and startup initializer.
        /// </summary>
        /// <param name="services">Service collection to register RabbitMQ services into.</param>
        internal static void RegisterRabbitMqInfrastructureServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<RabbitMqConnectionManager>();
            _ = services.AddSingleton<RabbitMqTopologyInitializer>();
            _ = services.AddHostedService<RabbitMqStartupInitializer>();
        }

        /// <summary>
        /// Registers transit publishing runtime services and startup initializer.
        /// </summary>
        /// <param name="services">Service collection to register transit publishing services into.</param>
        internal static void RegisterTransitPublisherServices(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddSingleton<TransitPublisher>();
            _ = services.AddHostedService<TransitPublisherStartupInitializer>();
        }

        /// <summary>
        /// Registers certificate runtime services and periodic renewal hosted service.
        /// </summary>
        /// <param name="services">Service collection to register certificate services into.</param>
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
        /// Registers the control-plane hosted service.
        /// </summary>
        /// <param name="services">Service collection to register the control-plane hosted service into.</param>
        internal static void RegisterControlPlaneService(IServiceCollection services)
        {
            ArgumentNullException.ThrowIfNull(services);
            _ = services.AddHostedService<ControlPlaneService>();
        }

        /// <summary>
        /// Gets a description of the host shutdown timeout for logging.
        /// </summary>
        /// <param name="shutdownOptions">BackFiller shutdown configuration.</param>
        /// <returns>Human-readable description of the host shutdown timeout.</returns>
        internal static string GetHostShutdownTimeoutDescription(ShutdownOptions shutdownOptions)
        {
            ArgumentNullException.ThrowIfNull(shutdownOptions);
            return $"Generic Host shutdown timeout: {shutdownOptions.GracePeriodSeconds}s";
        }

        /// <summary>
        /// Configures host-level service behavior from validated BackFiller runtime settings.
        /// </summary>
        /// <param name="hostBuilder">The <see cref="HostApplicationBuilder"/> to configure.</param>
        /// <param name="runtimeOptions">The validated immutable runtime options snapshot produced by startup validation.</param>
        /// <param name="lifecycle">Optional authoritative lifecycle instance created before host build.</param>
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
        /// Configures logging, runtime registrations, host services, and builds the host for execution.
        /// </summary>
        /// <param name="builder">The host application builder with validated configuration.</param>
        /// <param name="runtimeOptions">The validated runtime options snapshot to register.</param>
        /// <param name="serviceLifecycle">The authoritative lifecycle instance to preserve through host composition and execution.</param>
        /// <returns>The built <see cref="IHost"/> instance ready to run.</returns>
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
