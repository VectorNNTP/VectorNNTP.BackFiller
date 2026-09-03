// <copyright file="LetsEncryptCertificateRenewalService.Loggers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Source-generated log declarations for the certificate-renewal background loop.
    /// </summary>
    internal sealed partial class LetsEncryptCertificateRenewalService
    {
        /// <summary>
        /// Defines the informational log emitted when the renewal service exits immediately because ACME management is disabled.
        /// </summary>
        [LoggerMessage(EventId = 1200, Level = LogLevel.Information, Message = "Certificate renewal service is disabled by configuration.")]
        private static partial void LogServiceDisabled(ILogger logger);

        /// <summary>
        /// Defines the informational log emitted after a renewal iteration issued and activated a replacement certificate.
        /// </summary>
        [LoggerMessage(EventId = 1201, Level = LogLevel.Information, Message = "Certificate renewal completed successfully.")]
        private static partial void LogRenewalSucceeded(ILogger logger);

        /// <summary>
        /// Defines the warning log emitted when a renewal iteration fails and the service will retry on a later interval.
        /// </summary>
        /// <param name="logger">Logger that receives the event.</param>
        /// <param name="exception">Failure captured for diagnostics. The exception is logged, not rethrown by this helper.</param>
        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning, Message = "Certificate renewal iteration failed; will retry on next interval.")]
        private static partial void LogRenewalIterationFailed(ILogger logger, Exception exception);
    }
}
