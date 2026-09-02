// <copyright file="LetsEncryptCertificateRenewalService.Loggers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Defines lets encrypt certificate renewal service and its lets encrypt certificate renewal service.loggers contract.
    /// </summary>
    internal sealed partial class LetsEncryptCertificateRenewalService
    {
                /// <summary>
        /// Coordinates log service disabled for lets encrypt certificate renewal service.loggers.
        /// </summary>
        [LoggerMessage(EventId = 1200, Level = LogLevel.Information, Message = "Certificate renewal service is disabled by configuration.")]
        private static partial void LogServiceDisabled(ILogger logger);

                /// <summary>
        /// Coordinates log renewal succeeded for lets encrypt certificate renewal service.loggers.
        /// </summary>
        [LoggerMessage(EventId = 1201, Level = LogLevel.Information, Message = "Certificate renewal completed successfully.")]
        private static partial void LogRenewalSucceeded(ILogger logger);

                /// <summary>
        /// Coordinates log renewal iteration failed for lets encrypt certificate renewal service.loggers.
        /// </summary>
        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning, Message = "Certificate renewal iteration failed; will retry on next interval.")]
        private static partial void LogRenewalIterationFailed(ILogger logger, Exception exception);
    }
}
