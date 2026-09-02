// <copyright file="Program.cs" company="Usenet Ninja">
// Copyright (c) Chris Knipe &lt;cknipe@opticnetworks.net&gt;. Licensed under the Apache License, Version 2.0 (see LICENSE).
// </copyright>

// Program.cs — Entry point for the VectorNNTP.Backfiller worker service.
//
// Orchestrates the startup pipeline and provides the top-level exception safety net:
//
//   Phase 0  — Process bootstrap: initialize build metadata, culture, bootstrap logging, and global exception handlers.
//   Phase 1  — Command-line dispatch: execute non-host commands early (--help, --version, diagnostics).
//   Phase 2  — Configuration load: create HostApplicationBuilder and resolve command paths requiring configuration.
//   Phase 3  — Startup validation: validate configuration, dependencies, and produce immutable runtime options snapshot.
//   Phase 4  — Host composition: configure Serilog, register runtime options and hosted services, and build DI container.
//   Phase 5  — Host run loop: start worker service execution until shutdown.

using Serilog;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Startup;
using VectorNNTP.Backfiller.Startup.Commands;

namespace VectorNNTP.Backfiller
{
    /// <summary>
    /// Supplies the process entry point for the backfiller host.
    /// </summary>
    /// <remarks>
    /// <para><b>Execution model:</b> This partial class now serves primarily as the entry point while startup responsibilities
    /// are delegated to focused Startup components and the remaining Program partials for hosting, Serilog, command validation,
    /// and the validation pipeline.</para>
    /// </remarks>
    internal static partial class Program
    {
        /// <summary>
        /// Creates and runs the worker host through sequential startup phases with full exception safety.
        /// </summary>
        /// <param name="args">Command-line arguments passed to host configuration and application startup.</param>
        /// <remarks>
        /// <para>Execution flow: parse operational command, build configuration, validate configuration and
        /// external dependencies, create immutable runtime options, configure logging and DI services, then
        /// start the host run loop.</para>
        /// <para>Startup validation is cancelable via Ctrl+C and uses explicit exit codes for parse,
        /// configuration, dependency, cancellation, and unexpected-failure outcomes.</para>
        /// </remarks>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task Main(string[] args)
        {
            ProcessBootstrapper.ConfigureBootstrapLogger();
            Log.Information("Starting host initialization.");

            DateTimeOffset processStartedAt = DateTimeOffset.UtcNow;
            BuildInfoService.InitializeBuildInfo(processStartedAt);
            ProcessBootstrapper.SetProcessCulture();
            BuildInfoService.LogBuildInfo();
            ProcessBootstrapper.LogThreadPoolConfiguration();
            ProcessBootstrapper.RegisterGlobalExceptionHandlers();

            // Get the service lifecycle early (initialized with Starting state)
            ServiceLifecycle? lifecycle = null;

            // hostStarted tracks whether StartAsync completed and the host entered steady-state execution.
            // Normal shutdown now returns cleanly from WaitForShutdownAsync(). An OperationCanceledException after this
            // point is treated as unexpected rather than the ordinary shutdown path.
            bool hostStarted = false;

            // Startup cancellation source for Ctrl+C/SIGINT during validation/build.
            using CancellationTokenSource startupCancellation = new();

            try
            {
                HostApplicationBuilder builder;
                ConfigurationValidationResult configValidationResult;
                DependencyValidationResult dependencyValidationResult;
                BackFillerRuntimeOptions? runtimeOptions;

                if (!OperationalCommandDispatcher.TryDispatchPreConfigurationCommand(
                        args,
                        out OperationalCommand? command,
                        out int? commandExitCode))
                {
                    Environment.ExitCode = commandExitCode ?? ExitCodePolicy.ExitCodeConfigurationFailure;
                    return;
                }

                // CreateApplicationBuilder registers the default configuration sources: appsettings.json,
                // appsettings.{Environment}.json, environment variables, command-line args, and user secrets (Development).
                builder = Host.CreateApplicationBuilder(args);

                if (command.HasValue)
                {
                    Environment.ExitCode = OperationalCommandDispatcher.DispatchPostConfigurationCommand(
                        command.Value,
                        builder.Configuration);
                    return;
                }

                // Transition to Validating state (configuration and dependency checks begin).
                // This startup-created lifecycle instance is also registered into DI so validation,
                // readiness, and shutdown all share one authoritative state machine.
                ServiceLifecycle tempLifecycle = new();
                tempLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "Starting configuration and dependency validation");
                lifecycle = tempLifecycle;

                // Support graceful cancellation during startup validation.
                void StartupCancelHandler(object? _, ConsoleCancelEventArgs e)
                {
                    e.Cancel = true; // Allow graceful unwind instead of hard process termination.
                    startupCancellation.Cancel();
                    Log.Warning("Shutdown requested during startup validation; cancelling startup pipeline.");
                }

                Console.CancelKeyPress += StartupCancelHandler;

                try
                {
                    // Load, validate, canonicalize, and snapshot startup runtime configuration + dependencies.
                    (configValidationResult, dependencyValidationResult, runtimeOptions) =
                        await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                            builder.Configuration,
                            dependencyTimeout: TimeSpan.FromSeconds(5),
                            cancellationToken: startupCancellation.Token).ConfigureAwait(false);
                }
                finally
                {
                    Console.CancelKeyPress -= StartupCancelHandler;
                }

                // Log configuration diagnostics (warnings and errors) and stop only on errors.
                ValidationLogging.LogConfigurationValidationErrors(configValidationResult);
                if (!configValidationResult.IsValid)
                {
                    lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Configuration validation failed");
                    Environment.ExitCode = ExitCodePolicy.ExitCodeConfigurationFailure;
                    return;
                }

                // Log dependency diagnostics and stop on dependency failures/errors.
                if (!dependencyValidationResult.IsValid || dependencyValidationResult.Warnings.Count > 0)
                {
                    ValidationLogging.LogDependencyValidationErrors(dependencyValidationResult);
                }

                if (!dependencyValidationResult.IsValid)
                {
                    lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Dependency validation failed");
                    Environment.ExitCode = ExitCodePolicy.ExitCodeDependencyFailure;
                    return;
                }

                if (runtimeOptions == null)
                {
                    lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Runtime options snapshot was not produced");
                    Environment.ExitCode = ExitCodePolicy.ExitCodeConfigurationFailure;
                    Log.Fatal("Startup validation completed without a runtime options snapshot.");
                    return;
                }

                ServiceLifecycle serviceLifecycle = lifecycle ?? throw new InvalidOperationException("Service lifecycle was not initialized.");

                // Validation passed; transition to Initializing (DI container and services).
                serviceLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "Configuration valid; initializing DI container and services");
                Log.Information("Validated certificate directory: {CertificateDirectory}", runtimeOptions.ValidatedCertificateDirectory);
                Log.Information(
                    "Shutdown configuration: Worker grace period: {WorkerGracePeriodSeconds}s | Generic Host timeout: {HostShutdownTimeoutSeconds}s | RabbitMQ drain timeout: {RabbitMqDrainTimeoutSeconds}s | External supervisor budget: configure systemd TimeoutStopSec > {HostShutdownTimeoutSeconds}s (recommended +30s minimum)",
                    runtimeOptions.ShutdownGracePeriodSeconds,
                    runtimeOptions.ShutdownGracePeriodSeconds,
                    runtimeOptions.RabbitMqMaximumShutdownDrainTimeoutSeconds);

                // Build() triggers all ValidateOnStart() validations -- configuration errors are caught here as
                // OptionsValidationException before any hosted service starts.
                IHost host = Startup.Hosting.HostComposer.ComposeHost(
                    builder,
                    runtimeOptions,
                    serviceLifecycle);

                await Startup.Hosting.HostLifetimeCoordinator.RunAsync(
                    host,
                    serviceLifecycle,
                    () => hostStarted = true).ConfigureAwait(false);

            }
            catch (OperationCanceledException ex) when (hostStarted)
            {
                // WaitForShutdownAsync() without an external cancellation token should complete normally during
                // orderly shutdown. If an OperationCanceledException escapes after host start, treat it as an
                // unexpected runtime cancellation rather than the ordinary stop path.
                Log.Fatal(ex, "VectorNNTP.BackFiller terminated unexpectedly -- cancellation escaped after host start");
                lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Unexpected cancellation after host start");
                Environment.ExitCode = ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
            catch (OperationCanceledException ex) when (!hostStarted)
            {
                // Cancellation before the host reaches steady state is a startup failure, not an orderly shutdown.
                Log.Fatal(ex, "VectorNNTP.BackFiller startup failed -- cancellation received before host started");
                lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Startup cancelled before host initialization");
                Environment.ExitCode = ExitCodePolicy.ExitCodeStartupFailure;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("validation failed"))
            {
                // Configuration or dependency validation failed - exit code already set above
                // Just swallow to continue to finally block
            }
            catch (Microsoft.Extensions.Options.OptionsValidationException ex)
            {
                // Configuration validation failed during Build().
                Log.Fatal(ex, "VectorNNTP.BackFiller configuration validation failed");
                lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "Build() configuration validation failed");
                Environment.ExitCode = ExitCodePolicy.ExitCodeConfigurationFailure;
            }
            catch (Exception ex)
            {
                // Unhandled exception from any phase (startup or runtime).
                Log.Fatal(ex, "VectorNNTP.BackFiller terminated unexpectedly");
                lifecycle?.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, $"Unhandled exception: {ex.GetType().Name}");
                Environment.ExitCode = ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
            finally
            {
                // Log lifecycle summary (for diagnostics and operational visibility)
                if (lifecycle != null)
                {
                    Log.Information("Application lifecycle summary: {Summary}", lifecycle.GetSummary());
                }

                string exitDescription = ExitCodePolicy.GetExitCodeDescription(Environment.ExitCode);

                if (Environment.ExitCode != ExitCodePolicy.ExitCodeNormalShutdown)
                {
                    Log.Warning(
                        "VectorNNTP.BackFiller shut down with {ExitDescription} (ExitCode={ExitCode})",
                        exitDescription, Environment.ExitCode);
                }
                else
                {
                    Log.Information(
                        "VectorNNTP.BackFiller shut down complete (ExitCode={ExitCode}, {ExitDescription})",
                        Environment.ExitCode, exitDescription);
                }

                // CloseAndFlushAsync guarantees all buffered log events reach sinks before the process exits.
                // Wrapped in try/catch because a sink failure must not replace the original exception context.
                try
                {
                    await Log.CloseAndFlushAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Best effort -- sink failure during shutdown cannot be meaningfully recovered or reported.
                }
            }
        }
    }
}

