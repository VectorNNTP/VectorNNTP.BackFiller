// <copyright file="LetsEncryptCertificateRenewalService.Loggers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Defines the lets encrypt certificate renewal service component and its contracts for this subsystem.
    /// </summary>
    internal sealed partial class LetsEncryptCertificateRenewalService
    {
        [LoggerMessage(EventId = 1200, Level = LogLevel.Information, Message = "Certificate renewal service is disabled by configuration.")]
        /// <summary>
        /// Performs the log service disabled operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogServiceDisabled(ILogger logger);

        [LoggerMessage(EventId = 1201, Level = LogLevel.Information, Message = "Certificate renewal completed successfully.")]
        /// <summary>
        /// Performs the log renewal succeeded operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogRenewalSucceeded(ILogger logger);

        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning, Message = "Certificate renewal iteration failed; will retry on next interval.")]
        /// <summary>
        /// Performs the log renewal iteration failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogRenewalIterationFailed(ILogger logger, Exception exception);
    }
}
