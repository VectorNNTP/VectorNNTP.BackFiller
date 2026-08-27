// <copyright file="LetsEncryptCertificateRenewalService.Loggers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    internal sealed partial class LetsEncryptCertificateRenewalService
    {
        [LoggerMessage(EventId = 1200, Level = LogLevel.Information, Message = "Certificate renewal service is disabled by configuration.")]
        private static partial void LogServiceDisabled(ILogger logger);

        [LoggerMessage(EventId = 1201, Level = LogLevel.Information, Message = "Certificate renewal completed successfully.")]
        private static partial void LogRenewalSucceeded(ILogger logger);

        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning, Message = "Certificate renewal iteration failed; will retry on next interval.")]
        private static partial void LogRenewalIterationFailed(ILogger logger, Exception exception);
    }
}
