using System.Globalization;
using Serilog;

using Serilog.Events;

namespace VectorNNTP.Backfiller.Startup
{
    /// <summary>
    /// Owns process-wide bootstrap initialization such as culture, bootstrap logging, thread-pool diagnostics, and global exception handlers.
    /// </summary>
    internal static class ProcessBootstrapper
    {
        /// <summary>
        /// Sets the process-wide culture to <see cref="CultureInfo.InvariantCulture"/>.
        /// </summary>
        internal static void SetProcessCulture()
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
            Log.Information(
                "Process culture set to InvariantCulture (number format: {NumberSample}, date format: {DateSample})",
                1234.56.ToString("F2", CultureInfo.InvariantCulture),
                DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Configures the Serilog bootstrap logger.
        /// </summary>
        internal static void ConfigureBootstrapLogger()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .CreateBootstrapLogger();
        }

        /// <summary>
        /// Logs the current ThreadPool configuration for observability.
        /// </summary>
        internal static void LogThreadPoolConfiguration()
        {
            int cpuCount = Environment.ProcessorCount;

            ThreadPool.GetMinThreads(out int minWorkers, out int minIocp);
            ThreadPool.GetMaxThreads(out int maxWorkers, out int maxIocp);
            ThreadPool.GetAvailableThreads(out int availableWorkers, out int availableIocp);

            Log.Information(
                "ThreadPool configuration: CPUs={CpuCount}, MinWorkers={MinWorkers}, MinIOCP={MinIOCP}, MaxWorkers={MaxWorkers}, MaxIOCP={MaxIOCP}, AvailableWorkers={AvailableWorkers}, AvailableIOCP={AvailableIOCP}",
                cpuCount,
                minWorkers,
                minIocp,
                maxWorkers,
                maxIocp,
                availableWorkers,
                availableIocp);
        }

        /// <summary>
        /// Registers global exception handlers for <see cref="AppDomain.UnhandledException"/> and
        /// <see cref="TaskScheduler.UnobservedTaskException"/>.
        /// </summary>
        /// <remarks>
        /// <para>Called outside the try/catch block in Program.cs to capture exceptions from all startup phases,
        /// including ThreadPool configuration. Both handlers are wrapped in try/catch/finally to ensure robustness
        /// in terminal contexts.</para>
        ///
        /// <para><b><see cref="AppDomain.UnhandledException"/>:</b> Fires when an exception escapes all user catch blocks
        /// on non-ThreadPool threads. <b>This is a safety net only — it indicates the service has already lost control.</b>
        /// The role of this handler is to log and flush before termination, not to prevent service failures.
        /// For service reliability, design component failures to report through explicit channels, with supervisors
        /// deciding recovery strategy (retry, degrade, circuit-break). Do NOT rely on unhandled exceptions to signal failures.
        /// Logs at <see cref="LogEventLevel.Fatal"/> because the process is terminating and must trigger alerting.
        /// <see cref="Log.CloseAndFlush"/> is called synchronously as the last opportunity to flush buffered events
        /// before exit. Always called in finally to guarantee flushing even if logging itself fails.</para>
        ///
        /// <para><b><see cref="TaskScheduler.UnobservedTaskException"/>:</b> Fires during GC finalization when a faulted
        /// <see cref="Task"/> is collected without observing its exception. <b>This indicates a programming defect</b>
        /// — a fire-and-forget Task without explicit ownership strategy (await, background task registry, channel, etc.).
        /// This handler is a <i>last-resort diagnostic only</i>, not part of normal error handling. Logs at
        /// <see cref="LogEventLevel.Error"/> (process continues). <b>Intentionally does NOT call SetObserved()</b>
        /// to allow the exception to be tracked in production telemetry and trigger alerting. Code review is required
        /// to find and fix the unobserved Task. Do NOT treat this handler as error recovery — it is a red flag.</para>
        ///
        /// <para><b>Exception safety:</b> Both handlers are wrapped in try/catch to ensure sink failures or
        /// <see cref="ObjectDisposedException"/> never propagate -- masking the original crash or crashing the finalizer
        /// thread would be worse than losing one log event.</para>
        ///
        /// <para><b>Log severity asymmetry:</b> Unhandled exceptions log at Fatal (process terminating).
        /// Unobserved tasks log at Error (process continues) to trigger alerting and code review — these are
        /// bugs that must be fixed in the service implementation.</para>
        /// </remarks>
        internal static void RegisterGlobalExceptionHandlers()
        {
            // AppDomain.UnhandledException -- process is terminating; flush synchronously as the last chance.
            AppDomain.CurrentDomain.UnhandledException += (_, e) =>
            {
                try
                {
                    if (e.ExceptionObject is Exception ex)
                    {
                        Log.Fatal(
                            ex,
                            "VectorNNTP.Backfiller fatal: unhandled exception on background thread (IsTerminating={IsTerminating})",
                            e.IsTerminating);
                    }
                    else
                    {
                        Log.Fatal(
                            "VectorNNTP.Backfiller fatal: non-exception unhandled object: {ExceptionObject} (IsTerminating={IsTerminating})",
                            e.ExceptionObject,
                            e.IsTerminating);
                    }
                }
                finally
                {
                    // CloseAndFlush is wrapped in try/finally because the process is terminating regardless --
                    // there is no caller to propagate a flush exception to.  The finally block guarantees the
                    // flush attempt even if Log.Fatal itself throws.
                    try
                    {
                        Log.CloseAndFlush();
                    }
                    catch
                    {
                        // Sink failure during terminal flush -- nothing to do.  Swallow to prevent masking
                        // the original unhandled exception in crash dumps.
                    }
                }
            };

            // TaskScheduler.UnobservedTaskException -- fires when a faulted Task is collected without observing
            // its exception. This indicates a programming defect (fire-and-forget without tracking). Log at Error
            // to trigger alerting. Intentionally do NOT call SetObserved() -- allow the exception to propagate
            // through telemetry so operators and developers must investigate and fix the code.
            // Do NOT treat this as error recovery; it is a red flag requiring code review.
            TaskScheduler.UnobservedTaskException += (_, e) =>
            {
                try
                {
                    Log.Error(
                        e.Exception,
                        "VectorNNTP.Backfiller: unobserved Task exception -- this is a programming defect. Missing await or untracked fire-and-forget Task. Code review required.");
                }
                catch
                {
                    // Sink failure -- swallow. Crashing the GC finalizer thread is worse than losing one log event.
                }

                // DO NOT call SetObserved(). Let telemetry track this as an error condition.
                // The service implementation must be fixed to properly track background work.
            };
        }
    }
}
