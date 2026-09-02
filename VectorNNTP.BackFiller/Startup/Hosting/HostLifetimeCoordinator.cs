// <copyright file="HostLifetimeCoordinator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: host lifetime coordinator in the startup hosting subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Runtime.Shutdown;

namespace VectorNNTP.Backfiller.Startup.Hosting
{
    /// <summary>
    /// Coordinates host start/run/stop lifecycle flow, environment-aware readiness publication, and shutdown transition signaling.
    /// </summary>
    /// <remarks>
    /// This coordinator bridges Generic Host lifecycle callbacks with <see cref="ServiceLifecycle"/>, structured hosting
    /// diagnostics, and optional systemd readiness/stopping notifications.
    /// </remarks>
    internal partial class HostLifetimeCoordinator
    {
        /// <summary>
        /// Host execution environments recognized by runtime detection logic.
        /// </summary>
        internal enum HostingEnvironment
        {
            WindowsService,
            Systemd,
            Container,
            Console,
        }

        /// <summary>
        /// Cached hosting-environment detection result for the current process lifetime.
        /// </summary>
        private static HostingEnvironment? _currentEnvironment;

        /// <summary>
        /// Detects and caches the active hosting environment using systemd, container, and Windows-service probes.
        /// </summary>
        /// <returns>The detected hosting environment for the current process.</returns>
        internal static HostingEnvironment DetectHostingEnvironment()
        {
            if (_currentEnvironment.HasValue)
            {
                return _currentEnvironment.Value;
            }

            if (IsRunningUnderSystemd())
            {
                _currentEnvironment = HostingEnvironment.Systemd;
                return _currentEnvironment.Value;
            }

            if (IsRunningInContainer())
            {
                _currentEnvironment = HostingEnvironment.Container;
                return _currentEnvironment.Value;
            }

            if (IsRunningAsWindowsService())
            {
                _currentEnvironment = HostingEnvironment.WindowsService;
                return _currentEnvironment.Value;
            }

            _currentEnvironment = HostingEnvironment.Console;
            return _currentEnvironment.Value;
        }

        /// <summary>
        /// Registers startup and stopping callbacks that coordinate lifecycle transitions and graceful shutdown signaling.
        /// </summary>
        /// <param name="hostLifetime">Host lifecycle event source used to attach start/stop callbacks.</param>
        /// <param name="gracefulShutdownTimeout">Configured shutdown grace period used when signaling coordinated shutdown.</param>
        /// <param name="shutdownCoordinator">Coordinator that propagates graceful-shutdown intent to runtime components.</param>
        /// <param name="lifecycle">Optional authoritative lifecycle state machine used for transition updates.</param>
        internal static void RegisterReadinessHook(
            IHostApplicationLifetime hostLifetime,
            TimeSpan gracefulShutdownTimeout,
            ShutdownCoordinator shutdownCoordinator,
            ServiceLifecycle? lifecycle)
        {
            RegisterReadinessHook(
                hostLifetime,
                gracefulShutdownTimeout,
                shutdownCoordinator,
                lifecycle,
                NullLogger<HostLifetimeCoordinator>.Instance,
                NullLogger.Instance);
        }

        /// <summary>
        /// Internal readiness-hook registration that accepts explicit loggers for production and testing callers.
        /// </summary>
        /// <param name="hostLifetime">Host lifecycle event source.</param>
        /// <param name="gracefulShutdownTimeout">Configured shutdown grace period.</param>
        /// <param name="shutdownCoordinator">Runtime shutdown coordinator.</param>
        /// <param name="lifecycle">Optional lifecycle state tracker.</param>
        /// <param name="logger">Primary coordinator logger for lifecycle diagnostics.</param>
        /// <param name="systemdNotifierLogger">Logger routed to systemd notification helper diagnostics.</param>
        /// <exception cref="ArgumentNullException">Thrown when required dependencies/loggers are <see langword="null"/>.</exception>
        private static void RegisterReadinessHook(
            IHostApplicationLifetime hostLifetime,
            TimeSpan gracefulShutdownTimeout,
            ShutdownCoordinator shutdownCoordinator,
            ServiceLifecycle? lifecycle,
            ILogger<HostLifetimeCoordinator> logger,
            ILogger systemdNotifierLogger)
        {
            ArgumentNullException.ThrowIfNull(hostLifetime);
            ArgumentNullException.ThrowIfNull(shutdownCoordinator);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(systemdNotifierLogger);

            _ = hostLifetime.ApplicationStarted.Register(
                () => OnApplicationStarted(gracefulShutdownTimeout, logger));

            _ = hostLifetime.ApplicationStopping.Register(() => OnApplicationStopping(gracefulShutdownTimeout, shutdownCoordinator, lifecycle, logger, systemdNotifierLogger));
        }

        /// <summary>
        /// Signals external readiness after the host has reached its Ready lifecycle milestone.
        /// </summary>
        internal static void SignalReadinessAfterStartupMetrics()
        {
            SignalReadinessAfterStartupMetrics(NullLogger.Instance);
        }

        /// <summary>
        /// Publishes external readiness notification for environments that require explicit readiness signaling.
        /// </summary>
        /// <param name="systemdNotifierLogger">Logger passed to systemd notifier diagnostics.</param>
        /// <exception cref="ArgumentNullException"><paramref name="systemdNotifierLogger"/> is <see langword="null"/>.</exception>
        private static void SignalReadinessAfterStartupMetrics(ILogger systemdNotifierLogger)
        {
            ArgumentNullException.ThrowIfNull(systemdNotifierLogger);

            if (DetectHostingEnvironment() == HostingEnvironment.Systemd)
            {
                SystemdNotifier.NotifySystemdReady(systemdNotifierLogger);
            }
        }

        /// <summary>
        /// Logs the current hosting environment.
        /// </summary>
        internal static void LogHostingEnvironment()
        {
            LogHostingEnvironment(NullLogger<HostLifetimeCoordinator>.Instance);
        }

        /// <summary>
        /// Emits environment-detection diagnostics and environment-specific operational guidance logs.
        /// </summary>
        /// <param name="logger">Coordinator logger used for hosting-environment diagnostics.</param>
        /// <exception cref="ArgumentNullException"><paramref name="logger"/> is <see langword="null"/>.</exception>
        private static void LogHostingEnvironment(ILogger<HostLifetimeCoordinator> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);

            HostingEnvironment env = DetectHostingEnvironment();
            LogHostingEnvironmentDetected(logger, env);

            if (env == HostingEnvironment.Systemd)
            {
                LogSystemdDetected(logger);
            }
            else if (env == HostingEnvironment.Container)
            {
                LogContainerDetected(logger);
            }
            else if (env == HostingEnvironment.WindowsService)
            {
                LogWindowsServiceDetected(logger);
            }
        }

        /// <summary>
        /// Logs the shutdown policy using structured log properties.
        /// </summary>
        /// <param name="shutdownOptions">Shutdown policy options.</param>
        internal static void LogShutdownPolicy(ShutdownOptions shutdownOptions)
        {
            LogShutdownPolicy(shutdownOptions, NullLogger<HostLifetimeCoordinator>.Instance);
        }

        /// <summary>
        /// Emits structured shutdown-policy diagnostics from validated shutdown options.
        /// </summary>
        /// <param name="shutdownOptions">Validated shutdown policy options.</param>
        /// <param name="logger">Coordinator logger receiving shutdown-policy diagnostics.</param>
        /// <exception cref="ArgumentNullException"><paramref name="shutdownOptions"/> or <paramref name="logger"/> is <see langword="null"/>.</exception>
        private static void LogShutdownPolicy(ShutdownOptions shutdownOptions, ILogger<HostLifetimeCoordinator> logger)
        {
            ArgumentNullException.ThrowIfNull(shutdownOptions);
            ArgumentNullException.ThrowIfNull(logger);

            LogShutdownPolicyCore(
                logger,
                shutdownOptions.GracePeriodSeconds,
                true,
                shutdownOptions.FinishActiveArticles,
                shutdownOptions.DrainQueuedWork);
        }

        /// <summary>
        /// Logs the shutdown policy from the authoritative runtime configuration snapshot.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime configuration snapshot.</param>
        internal static void LogShutdownPolicy(BackFillerRuntimeOptions runtimeOptions)
        {
            LogShutdownPolicy(runtimeOptions, NullLogger<HostLifetimeCoordinator>.Instance);
        }

        /// <summary>
        /// Emits structured shutdown-policy diagnostics from the immutable runtime options snapshot.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options snapshot containing shutdown settings.</param>
        /// <param name="logger">Coordinator logger receiving shutdown-policy diagnostics.</param>
        /// <exception cref="ArgumentNullException"><paramref name="runtimeOptions"/> or <paramref name="logger"/> is <see langword="null"/>.</exception>
        private static void LogShutdownPolicy(BackFillerRuntimeOptions runtimeOptions, ILogger<HostLifetimeCoordinator> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(logger);

            LogShutdownPolicyCore(
                logger,
                runtimeOptions.ShutdownGracePeriodSeconds,
                true,
                runtimeOptions.ShutdownFinishActiveArticles,
                runtimeOptions.ShutdownDrainQueuedWork);
        }

        /// <summary>
        /// Runs the built host, coordinates readiness publication, waits for shutdown, and finalizes lifecycle terminal transitions.
        /// </summary>
        /// <param name="host">Built host instance to start, monitor, and dispose.</param>
        /// <param name="serviceLifecycle">Authoritative lifecycle state machine updated across startup and shutdown milestones.</param>
        /// <param name="markHostStarted">Callback invoked after readiness decision and immediately before shutdown wait begins.</param>
        /// <returns>A task that completes after host shutdown and post-run lifecycle reconciliation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="host"/>, <paramref name="serviceLifecycle"/>, or <paramref name="markHostStarted"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// Readiness publication is suppressed if shutdown has already been requested. In that case the lifecycle transitions
        /// directly toward draining semantics instead of advertising ready state.
        /// </remarks>
        internal static async Task RunAsync(IHost host, ServiceLifecycle serviceLifecycle, Action markHostStarted)
        {
            ArgumentNullException.ThrowIfNull(host);
            ArgumentNullException.ThrowIfNull(serviceLifecycle);
            ArgumentNullException.ThrowIfNull(markHostStarted);

            using (host)
            {
                ILogger<HostLifetimeCoordinator> logger = host.Services.GetRequiredService<ILogger<HostLifetimeCoordinator>>();
                ILogger systemdNotifierLogger = host.Services
                    .GetRequiredService<ILoggerFactory>()
                    .CreateLogger(typeof(SystemdNotifier).FullName ?? nameof(SystemdNotifier));

                LogHostingEnvironment(logger);
                BackFillerRuntimeOptions runtimeSnapshot = host.Services.GetRequiredService<BackFillerRuntimeOptions>();
                TimeSpan gracefulShutdownTimeout = TimeSpan.FromSeconds(runtimeSnapshot.ShutdownGracePeriodSeconds);
                ShutdownCoordinator shutdownCoordinator = host.Services.GetRequiredService<ShutdownCoordinator>();
                IHostApplicationLifetime hostLifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
                LogShutdownPolicy(runtimeSnapshot, logger);
                RegisterReadinessHook(
                    hostLifetime,
                    gracefulShutdownTimeout,
                    shutdownCoordinator,
                    serviceLifecycle,
                    logger,
                    systemdNotifierLogger);

                await host.StartAsync().ConfigureAwait(false);

                if (ShouldPublishReadiness(hostLifetime))
                {
                    serviceLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "Host started; ready to process work");

                    if (ShouldPublishReadiness(hostLifetime))
                    {
                        SignalReadinessAfterStartupMetrics(systemdNotifierLogger);
                    }
                    else
                    {
                        LogReadinessSuppressedDueToShutdown(logger);
                        serviceLifecycle.TransitionTo(
                            ServiceLifecycle.LifecycleState.Draining,
                            "Shutdown signal received during startup readiness publication; startup readiness suppressed and shutdown drain continues");
                    }
                }
                else
                {
                    LogReadinessSuppressedDueToShutdown(logger);

                    if (serviceLifecycle.CurrentState == ServiceLifecycle.LifecycleState.Initializing)
                    {
                        serviceLifecycle.TransitionTo(
                            ServiceLifecycle.LifecycleState.Draining,
                            "Host start completed after shutdown signal; startup readiness suppressed and shutdown drain continues");
                    }
                }

                markHostStarted();
                await host.WaitForShutdownAsync().ConfigureAwait(false);
            }

            if (serviceLifecycle.CurrentState == ServiceLifecycle.LifecycleState.Draining)
            {
                serviceLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Stopped, "Host shutdown completed");
            }
            else if (serviceLifecycle.CurrentState == ServiceLifecycle.LifecycleState.Initializing)
            {
                serviceLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Host shutdown completed before startup readiness transition");
            }
        }

        /// <summary>
        /// Handles is running under systemd for host lifetime coordinator.
        /// </summary>
        private static bool IsRunningUnderSystemd()
        {
            try
            {
                string? listenPid = Environment.GetEnvironmentVariable("LISTEN_PID");
                string? invocationId = Environment.GetEnvironmentVariable("INVOCATION_ID");

                return (!string.IsNullOrEmpty(listenPid) && listenPid == Environment.ProcessId.ToString())
                       || !string.IsNullOrEmpty(invocationId);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Handles should publish readiness for host lifetime coordinator.
        /// </summary>
        private static bool ShouldPublishReadiness(IHostApplicationLifetime hostLifetime)
        {
            ArgumentNullException.ThrowIfNull(hostLifetime);
            return !hostLifetime.ApplicationStopping.IsCancellationRequested;
        }

        /// <summary>
        /// Exposes readiness-publication gating logic for tests.
        /// </summary>
        /// <param name="hostLifetime">Host lifecycle instance whose stopping token determines readiness eligibility.</param>
        /// <returns><see langword="true"/> when readiness publication is still allowed; otherwise <see langword="false"/>.</returns>
        internal static bool ShouldPublishReadinessForTesting(IHostApplicationLifetime hostLifetime)
        {
            return ShouldPublishReadiness(hostLifetime);
        }

        /// <summary>
        /// Handles is running in container for host lifetime coordinator.
        /// </summary>
        private static bool IsRunningInContainer()
        {
            try
            {
                if (Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true")
                {
                    return true;
                }

                if (Directory.Exists("/.dockerenv"))
                {
                    return true;
                }

                if (File.Exists("/proc/1/cgroup"))
                {
                    string cgroup = File.ReadAllText("/proc/1/cgroup");
                    if (cgroup.Contains("docker") || cgroup.Contains("kubelet") || cgroup.Contains("lxc"))
                    {
                        return true;
                    }
                }

                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Handles is running as windows service for host lifetime coordinator.
        /// </summary>
        private static bool IsRunningAsWindowsService()
        {
            try
            {
                return System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) && WindowsServiceHelpers.IsWindowsService();
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Handles ApplicationStarted by emitting startup milestone diagnostics with environment and shutdown-budget context.
        /// </summary>
        /// <param name="gracefulShutdownTimeout">Configured graceful shutdown timeout used for operational context logging.</param>
        /// <param name="logger">Coordinator logger receiving startup milestone diagnostics.</param>
        /// <exception cref="ArgumentNullException"><paramref name="logger"/> is <see langword="null"/>.</exception>
        private static void OnApplicationStarted(TimeSpan gracefulShutdownTimeout, ILogger<HostLifetimeCoordinator> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);

            HostingEnvironment env = DetectHostingEnvironment();
            LogApplicationStarted(
                logger,
                env,
                gracefulShutdownTimeout.TotalSeconds);
        }

        /// <summary>
        /// Handles ApplicationStopping by transitioning lifecycle state, issuing optional systemd STOPPING notification, and signaling coordinated shutdown.
        /// </summary>
        /// <param name="gracefulShutdownTimeout">Configured shutdown grace period used when signaling runtime shutdown.</param>
        /// <param name="shutdownCoordinator">Coordinator responsible for broadcasting graceful-shutdown intent.</param>
        /// <param name="lifecycle">Optional lifecycle state machine updated for shutdown transition visibility.</param>
        /// <param name="logger">Coordinator logger receiving shutdown transition diagnostics.</param>
        /// <param name="systemdNotifierLogger">Logger used by systemd notifier diagnostics.</param>
        /// <exception cref="ArgumentNullException">Thrown when required shutdown/logging dependencies are <see langword="null"/>.</exception>
        private static void OnApplicationStopping(
            TimeSpan gracefulShutdownTimeout,
            ShutdownCoordinator shutdownCoordinator,
            ServiceLifecycle? lifecycle,
            ILogger<HostLifetimeCoordinator> logger,
            ILogger systemdNotifierLogger)
        {
            ArgumentNullException.ThrowIfNull(shutdownCoordinator);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(systemdNotifierLogger);

            HostingEnvironment env = DetectHostingEnvironment();
            LogShutdownSignalReceived(
                logger,
                env,
                gracefulShutdownTimeout);

            if (lifecycle?.CurrentState == ServiceLifecycle.LifecycleState.Ready)
            {
                try
                {
                    lifecycle.TransitionTo(
                        ServiceLifecycle.LifecycleState.Draining,
                        "Host stopping signal received; draining in-flight work");
                }
                catch (InvalidOperationException ex)
                {
                    ServiceLifecycle.LifecycleState current = lifecycle.CurrentState;

                    if (current is ServiceLifecycle.LifecycleState.Draining or
                        ServiceLifecycle.LifecycleState.Stopped)
                    {
                        LogConcurrentLifecycleTransitionRace(
                            logger,
                            current);
                    }
                    else if (current == ServiceLifecycle.LifecycleState.Faulted)
                    {
                        LogApplicationStoppingAlreadyFaulted(
                            logger,
                            ex,
                            current);
                    }
                    else if (current == ServiceLifecycle.LifecycleState.Ready)
                    {
                        throw;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            if (env == HostingEnvironment.Systemd)
            {
                SystemdNotifier.NotifySystemdStopping(systemdNotifierLogger);
            }

            shutdownCoordinator.SignalGracefulShutdown(gracefulShutdownTimeout);
        }

        /// <summary>
        /// Emits informational hosting-environment classification with structured <c>Environment</c> value.
        /// </summary>
        [LoggerMessage(EventId = 1001, Level = LogLevel.Information, Message = "Hosting environment: {Environment}")]
        private static partial void LogHostingEnvironmentDetected(ILogger logger, HostingEnvironment environment);

        /// <summary>
        /// Emits the systemd detected log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1002, Level = LogLevel.Information, Message = "systemd detected: Type=notify will expect READY notification; set TimeoutStartSec to allow time for dependencies and hosted service initialization")]
        private static partial void LogSystemdDetected(ILogger logger);

        /// <summary>
        /// Emits the container detected log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1003, Level = LogLevel.Information, Message = "Container environment detected; ensure orchestration platform can monitor process health and exit codes (see EXIT_CODES_AND_SYSTEMD.md)")]
        private static partial void LogContainerDetected(ILogger logger);

        /// <summary>
        /// Emits the windows service detected log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1004, Level = LogLevel.Information, Message = "Windows Service detected; ensure graceful shutdown timeout is sufficient for cleanup")]
        private static partial void LogWindowsServiceDetected(ILogger logger);

        /// <summary>
        /// Emits structured informational shutdown-policy diagnostics consumed during startup environment reporting.
        /// </summary>
        [LoggerMessage(EventId = 1005, Level = LogLevel.Information, Message = "Shutdown policy: GracePeriodSeconds={GracePeriodSeconds}, StopNewWorkAdmission={StopNewWorkAdmission}, FinishActiveArticles={FinishActiveArticles}, DrainQueuedWork={DrainQueuedWork}")]
        private static partial void LogShutdownPolicyCore(
            ILogger logger,
            int gracePeriodSeconds,
            bool stopNewWorkAdmission,
            bool finishActiveArticles,
            bool drainQueuedWork);

        /// <summary>
        /// Emits the application started log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1006, Level = LogLevel.Information, Message = "Host startup milestone reached (ApplicationStarted); Program.Main will publish readiness after StartAsync completes; hosting environment={Environment}; graceful shutdown timeout={ShutdownTimeout}s")]
        private static partial void LogApplicationStarted(
            ILogger logger,
            HostingEnvironment environment,
            double shutdownTimeout);

        /// <summary>
        /// Emits the shutdown signal received log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1007, Level = LogLevel.Information, Message = "Shutdown signal received; establishing shutdown state; hosting environment={Environment}; gracePeriod={GracePeriod}")]
        private static partial void LogShutdownSignalReceived(
            ILogger logger,
            HostingEnvironment environment,
            TimeSpan gracePeriod);

        /// <summary>
        /// Emits the concurrent lifecycle transition race log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1008, Level = LogLevel.Debug, Message = "Concurrent lifecycle transition race: attempted Ready->Draining but observed {CurrentState}; treating as benign.")]
        private static partial void LogConcurrentLifecycleTransitionRace(
            ILogger logger,
            ServiceLifecycle.LifecycleState currentState);

        /// <summary>
        /// Emits the application stopping already faulted log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1009, Level = LogLevel.Warning, Message = "ApplicationStopping observed but lifecycle already Faulted (state={CurrentState}); proceeding with shutdown.")]
        private static partial void LogApplicationStoppingAlreadyFaulted(
            ILogger logger,
            Exception exception,
            ServiceLifecycle.LifecycleState currentState);

        /// <summary>
        /// Emits the readiness suppressed due to shutdown log event for host lifetime coordinator.
        /// </summary>
        [LoggerMessage(EventId = 1010, Level = LogLevel.Information, Message = "Suppressing readiness publication because shutdown is already in progress.")]
        private static partial void LogReadinessSuppressedDueToShutdown(ILogger logger);
    }
}
