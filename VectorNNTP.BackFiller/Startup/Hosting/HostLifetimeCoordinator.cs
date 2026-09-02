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
    /// Owns host runtime execution, hosting-environment detection, readiness signaling, lifecycle transitions, systemd signaling, and shutdown coordination.
    /// </summary>
    internal partial class HostLifetimeCoordinator
    {
        /// <summary>
        /// Supported hosting environments.
        /// </summary>
        internal enum HostingEnvironment
        {
            WindowsService,
            Systemd,
            Container,
            Console,
        }

        /// <summary>
        /// Stores current environment used by host lifetime coordinator.
        /// </summary>
        private static HostingEnvironment? _currentEnvironment;

        /// <summary>
        /// Detects the current hosting environment.
        /// </summary>
        /// <returns>The operation result.</returns>
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
        /// Registers host lifetime callbacks for startup/shutdown coordination.
        /// </summary>
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
        /// Handles register readiness hook for host lifetime coordinator.
        /// </summary>
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
        /// Handles signal readiness after startup metrics for host lifetime coordinator.
        /// </summary>
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
        /// Emits the hosting environment log event for host lifetime coordinator.
        /// </summary>
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
        /// Emits the shutdown policy log event for host lifetime coordinator.
        /// </summary>
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
        /// Emits the shutdown policy log event for host lifetime coordinator.
        /// </summary>
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
        /// Executes the built host through startup, steady-state waiting, disposal, and final stopped transition.
        /// </summary>
        /// <param name="host">The built host instance.</param>
        /// <param name="serviceLifecycle">The authoritative service lifecycle instance.</param>
        /// <param name="markHostStarted">Callback invoked immediately before entering steady-state shutdown wait.</param>
        /// <returns>A task that completes when the host shuts down cleanly.</returns>
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
        /// Determines whether readiness should be published for the current test host.
        /// </summary>
        /// <param name="hostLifetime">The hostLifetime value.</param>
        /// <returns>true when the operation succeeds; otherwise false.</returns>
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
        /// Handles on application started for host lifetime coordinator.
        /// </summary>
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
        /// Handles on application stopping for host lifetime coordinator.
        /// </summary>
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
        /// Emits the hosting environment detected log event for host lifetime coordinator.
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
        /// Emits the shutdown policy core log event for host lifetime coordinator.
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
