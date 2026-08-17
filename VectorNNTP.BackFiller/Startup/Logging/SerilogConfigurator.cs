using Serilog;
using Serilog.Events;

namespace VectorNNTP.Backfiller.Startup.Logging
{
    /// <summary>
    /// Owns production Serilog pipeline configuration and bootstrap-to-production logger transition.
    /// </summary>
    internal static class SerilogConfigurator
    {
        private const string LogFilePrefix = "vectornntp.backfiller-.log";

        /// <summary>
        /// Configures and registers the production Serilog pipeline.
        /// </summary>
        /// <param name="services">The service collection to register Serilog against.</param>
        /// <param name="configuration">The application configuration root.</param>
        /// <param name="serviceName">The service identity property value.</param>
        /// <param name="validatedLogDirectory">Validated absolute log directory path.</param>
        internal static void ConfigureSerilogLogging(
            IServiceCollection services,
            IConfiguration configuration,
            string serviceName,
            string validatedLogDirectory)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);
            ArgumentException.ThrowIfNullOrWhiteSpace(validatedLogDirectory);

            Serilog.ILogger bootstrapLogger = Log.Logger;
            Serilog.ILogger? productionLogger = null;

            string logFilePath = Path.Combine(validatedLogDirectory, LogFilePrefix);
            LogEventLevel minimumLevel = ParseMinimumLevel(configuration["Serilog:MinimumLevel:Default"]);

            try
            {
                productionLogger = new LoggerConfiguration()
                    .MinimumLevel.Is(minimumLevel)
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Information)
                    .MinimumLevel.Override("System", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .Enrich.WithProperty("Application", serviceName)
                    .Enrich.WithProperty("ProcessId", Environment.ProcessId)
                    .WriteTo.Async(
                        configure: sink => sink.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}"),
                        blockWhenFull: true)
                    .WriteTo.Async(
                        configure: sink => sink.File(
                            path: logFilePath,
                            rollingInterval: RollingInterval.Day,
                            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
                            retainedFileCountLimit: 30,
                            fileSizeLimitBytes: 1073741824,
                            rollOnFileSizeLimit: true),
                        blockWhenFull: true)
                    .CreateLogger();

                _ = services.AddLogging(logging =>
                {
                    _ = logging.ClearProviders();
                    _ = logging.SetMinimumLevel(ParseMicrosoftLogLevel(configuration["Serilog:MinimumLevel:Default"]));
                    _ = logging.AddSerilog(productionLogger, dispose: false);
                });
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Failed to configure production Serilog pipeline.");

                if (productionLogger is IDisposable disposableProductionLogger)
                {
                    disposableProductionLogger.Dispose();
                }

                throw;
            }

            Log.Logger = productionLogger;
            Log.Information("Serilog file sink initialized at validated directory: {LogDirectory}", validatedLogDirectory);

            if (!ReferenceEquals(bootstrapLogger, productionLogger) &&
                bootstrapLogger is IDisposable bootstrapDisposable)
            {
                try
                {
                    bootstrapDisposable.Dispose();
                }
                catch (Exception ex)
                {
                    Log.Warning(ex, "Failed to dispose bootstrap logger during logger swap.");
                }
            }
        }

        internal static LogEventLevel ParseMinimumLevelForTesting(string? configuredLevel)
        {
            return ParseMinimumLevel(configuredLevel);
        }

        internal static LogLevel ParseMicrosoftLogLevelForTesting(string? configuredLevel)
        {
            return ParseMicrosoftLogLevel(configuredLevel);
        }

        private static LogEventLevel ParseMinimumLevel(string? configuredLevel)
        {
            return Enum.TryParse(configuredLevel, ignoreCase: true, out LogEventLevel level)
                ? level
                : LogEventLevel.Information;
        }

        private static LogLevel ParseMicrosoftLogLevel(string? configuredLevel)
        {
            if (string.IsNullOrWhiteSpace(configuredLevel))
            {
                return LogLevel.Information;
            }

            if (Enum.TryParse(configuredLevel, ignoreCase: true, out LogLevel level))
            {
                return level;
            }

            return configuredLevel.Trim().ToLowerInvariant() switch
            {
                "verbose" => LogLevel.Trace,
                "fatal" => LogLevel.Critical,
                _ => LogLevel.Information,
            };
        }
    }
}
