// <copyright file="SystemdNotifier.cs" company="Usenet Ninja">
// Copyright (c) Chris Knipe <cknipe@opticnetworks.net>. Licensed under the Apache License, Version 2.0 (see LICENSE).
// </copyright>

// Systemd notification integration.
//
// Provides the application-side notification bridge to systemd (READY/STOPPING/STATUS)
// for Linux service deployments. Service policy and lifecycle configuration belong in
// the systemd unit file, not this source module.
//
// Application-provided systemd integration in this module:
//   - READY=1 notification helper for Type=notify services
//   - STOPPING=1 notification helper for graceful shutdown state
//   - STATUS=... notification helper for coarse runtime status visibility (rate-limited)
//
// STATUS notifications are intended for human-readable operational state, not high-frequency telemetry.
// Metrics and high-cardinality runtime data should go to OpenTelemetry/Serilog, not sd_notify datagrams.
//
// Note: unit-file directives (RestartForceExitStatus, RestartPreventExitStatus,
// TimeoutStartSec/TimeoutStopSec, After/Wants, etc.) are deployment configuration
// and should be documented with the .service file rather than in application source.
//
// Threading/lifecycle: sd_notify helpers are invoked from startup/shutdown lifecycle transitions.
// Shared state used for STATUS throttling and library availability caching is synchronized for
// safe concurrent invocation.

using System.Runtime.InteropServices;

namespace VectorNNTP.Backfiller.Startup.Hosting
{
    /// <summary>
    /// Systemd notification helpers for service lifecycle signaling.
    /// </summary>
    /// <remarks>
    /// <para><b>Purpose:</b> Provide the application-side bridge for systemd notifications
    /// (READY/STOPPING/STATUS) during Linux service operation.</para>
    ///
    /// <para><b>Usage:</b> Call <see cref="NotifySystemdReady"/> after hosted services have started
    /// and are ready to accept work. Optionally call <see cref="NotifySystemdStopping"/> before
    /// graceful shutdown.</para>
    /// </remarks>
    internal static partial class SystemdNotifier
    {
        /// <summary>
        /// Stores the min status notify interval milliseconds state used to enforce this component's runtime contract.
        /// </summary>
        private const int MinStatusNotifyIntervalMilliseconds = 5000;
        /// <summary>
        /// Stores the max systemd status length state used to enforce this component's runtime contract.
        /// </summary>
        private const int MaxSystemdStatusLength = 1024;
        /// <summary>
        /// Stores the systemd library state unknown state used to enforce this component's runtime contract.
        /// </summary>
        private const int SystemdLibraryStateUnknown = 0;
        /// <summary>
        /// Stores the systemd library state available state used to enforce this component's runtime contract.
        /// </summary>
        private const int SystemdLibraryStateAvailable = 1;
        /// <summary>
        /// Stores the systemd library state unavailable state used to enforce this component's runtime contract.
        /// </summary>
        private const int SystemdLibraryStateUnavailable = 2;

        /// <summary>
        /// Stores the status notify gate state used to enforce this component's runtime contract.
        /// </summary>
        private static readonly object StatusNotifyGate = new();
        /// <summary>
        /// Stores the last status notify tick count state used to enforce this component's runtime contract.
        /// </summary>
        private static long _lastStatusNotifyTickCount = -1;
        /// <summary>
        /// Stores the systemd library state state used to enforce this component's runtime contract.
        /// </summary>
        private static int _systemdLibraryState = SystemdLibraryStateUnknown;

        #region Systemd Notifications

        /// <summary>
        /// Signals to systemd that the service is ready (Type=notify services only).
        /// </summary>
        /// <remarks>
        /// <para>Sends <c>READY=1</c> notification to systemd. Systemd will not mark the service
        /// as "started" until this notification is received (when <c>Type=notify</c> is set in the
        /// service file). This prevents dependent services from starting before this service is
        /// truly ready to accept work.</para>
        ///
        /// <para>Safe to call multiple times or on non-systemd environments (no-op on Windows/when
        /// environment variable is not set).</para>
        /// </remarks>
        public static void NotifySystemdReady(ILogger logger)
        {
            if (NotifySystemd("READY=1", "readiness", logger))
            {
                LogSystemdReadyNotified(logger);
            }
        }

        /// <summary>
        /// Signals to systemd that the service is stopping gracefully (Type=notify services only).
        /// </summary>
        /// <remarks>
        /// <para>Sends <c>STOPPING=1</c> notification to systemd, indicating graceful shutdown is
        /// in progress. systemd can use this state information while managing the service.</para>
        ///
        /// <para>Optional; systemd will infer stopping state from process termination if not notified.</para>
        /// </remarks>
        public static void NotifySystemdStopping(ILogger logger)
        {
            if (NotifySystemd("STOPPING=1", "stopping", logger))
            {
                LogSystemdStoppingNotified(logger);
            }
        }

        /// <summary>
        /// Sends a custom status message to systemd (Type=notify services only).
        /// </summary>
        /// <param name="statusMessage">Status message (e.g., "Running: 38.7 Gbps, 128k articles/s").</param>
        /// <param name="logger">Logger used for operational diagnostics.</param>
        /// <remarks>
        /// <para>Sends <c>STATUS=&lt;message&gt;</c> to systemd. This appears in <c>systemctl status</c>
        /// output and systemd logs, providing service status visibility.</para>
        ///
        /// <para>STATUS updates are rate-limited and intended for coarse, human-readable operational
        /// state (for example, every few seconds). High-frequency telemetry belongs in metrics/logging.</para>
        /// <para><b>Throttling:</b> Only successful notifications consume the throttle slot. Transient
        /// failures do not suppress subsequent attempts.</para>
        /// </remarks>
        public static void NotifySystemdStatus(string statusMessage, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(statusMessage);

            if (!IsSystemdNotifyAvailable())
            {
                return;
            }

            string normalizedStatusMessage = NormalizeSystemdStatus(statusMessage);
            if (string.IsNullOrWhiteSpace(normalizedStatusMessage))
            {
                return;
            }

            // Serialize STATUS notification to ensure atomic throttle-check-send-update sequence.
            // sd_notify() is a fast Unix-domain datagram operation, so holding the lock is acceptable.
            lock (StatusNotifyGate)
            {
                long nowTickCount = Environment.TickCount64;

                // Throttle: only allow STATUS notification if minimum interval has elapsed since last *successful* send
                if (_lastStatusNotifyTickCount >= 0 &&
                    nowTickCount - _lastStatusNotifyTickCount < MinStatusNotifyIntervalMilliseconds)
                {
                    return;
                }

                // Attempt notification; only update throttle timestamp on success
                if (NotifySystemd($"STATUS={normalizedStatusMessage}", "status", logger))
                {
                    _lastStatusNotifyTickCount = nowTickCount;
                    LogSystemdStatusNotified(logger, normalizedStatusMessage);
                }
                // On failure, throttle is NOT consumed; next caller may retry immediately
            }
        }

        #endregion

        #region Systemd Environment

        /// <summary>
        /// Normalizes a systemd STATUS value to a bounded single-line status payload.
        /// </summary>
        /// <param name="statusMessage">Raw status text supplied by the caller.</param>
        /// <returns>Normalized single-line status text, truncated to the configured maximum length.</returns>
        private static string NormalizeSystemdStatus(string statusMessage)
        {
            ArgumentNullException.ThrowIfNull(statusMessage);

            // Normalize line endings: treat CRLF as a single logical newline
            string normalized = statusMessage
                .Replace('\0', ' ')
                .Replace("\r\n", " ")
                .Replace('\r', ' ')
                .Replace('\n', ' ');

            if (normalized.Length <= MaxSystemdStatusLength)
            {
                return normalized;
            }

            // Truncate to maximum length, ensuring we don't split a surrogate pair
            string result = normalized[..MaxSystemdStatusLength];

            // If the last character is a high surrogate (first half of a pair), remove it
            // to avoid producing an invalid UTF-16 string
            if (char.IsHighSurrogate(result[^1]))
            {
                result = result[..^1];
            }

            return result;
        }

        /// <summary>
        /// Checks whether the systemd notify socket is available.
        /// </summary>
        /// <returns><see langword="true"/> if systemd notification is available; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// <para>Checks for the <c>NOTIFY_SOCKET</c> environment variable set by systemd when the service
        /// runs under <c>Type=notify</c>. Returns <see langword="false"/> on non-Linux platforms or when not running under systemd.</para>
        /// </remarks>
        private static bool IsSystemdNotifyAvailable()
        {
            return OperatingSystem.IsLinux()
                && !string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable("NOTIFY_SOCKET"));
        }

        /// <summary>
        /// Sends a notification to systemd via sd_notify().
        /// </summary>
        /// <param name="state">Notification state string (e.g., "READY=1", "STOPPING=1", "STATUS=...").</param>
        /// <param name="notificationType">Diagnostic label for the notification type (e.g., "readiness", "status").</param>
        /// <param name="logger">Logger used for diagnostics related to notify delivery.</param>
        /// <returns><see langword="true"/> if the notification was sent successfully; otherwise <see langword="false"/>.</returns>
        private static bool NotifySystemd(string state, string notificationType, ILogger logger)
        {
            if (!IsSystemdNotifyAvailable())
            {
                return false;
            }

            // Avoid repeated native calls after a known permanent library/entry-point failure
            int libraryState = Volatile.Read(ref _systemdLibraryState);
            if (libraryState == SystemdLibraryStateUnavailable)
            {
                return false;
            }

            try
            {
                int result = SdNotify(0, state);
                if (!IsSdNotifySuccess(result))
                {
                    LogSystemdNotificationFailedResult(logger, notificationType, result);
                    return false;
                }

                // Cache success: library is available
                Volatile.Write(ref _systemdLibraryState, SystemdLibraryStateAvailable);
                return true;
            }
            catch (DllNotFoundException)
            {
                // libsystemd.so.0 not installed; cache failure to avoid repeated attempts
                LogSystemdLibraryNotFound(logger, notificationType);
                Volatile.Write(ref _systemdLibraryState, SystemdLibraryStateUnavailable);
                return false;
            }
            catch (EntryPointNotFoundException)
            {
                // Library exists but sd_notify symbol missing; cache failure
                LogSystemdEntryPointNotFound(logger, notificationType);
                Volatile.Write(ref _systemdLibraryState, SystemdLibraryStateUnavailable);
                return false;
            }
            catch (Exception ex)
            {
                // Other unexpected errors (don't cache; might be transient)
                LogSystemdNotificationException(logger, ex, notificationType);
                return false;
            }
        }

        #endregion

        #region Testing Helpers

        /// <summary>
        /// Normalizes systemd status message for validation/testing (exposes internal normalization logic).
        /// </summary>
        /// <param name="status">Raw status string.</param>
        /// <returns>Normalized status message.</returns>
        internal static string NormalizeSystemdStatusForTesting(string status)
        {
            return NormalizeSystemdStatus(status);
        }

        /// <summary>
        /// Checks whether a STATUS notification would be allowed at the given time (for testing).
        /// </summary>
        /// <param name="nowTickCount">Current tick count to check against.</param>
        /// <returns><see langword="true"/> if notification would be allowed; otherwise <see langword="false"/> (throttled).</returns>
        internal static bool CanSendStatusNotificationForTesting(long nowTickCount)
        {
            lock (StatusNotifyGate)
            {
                return _lastStatusNotifyTickCount < 0 ||
                       nowTickCount - _lastStatusNotifyTickCount >= MinStatusNotifyIntervalMilliseconds;
            }
        }

        /// <summary>
        /// Updates the last notification timestamp for testing (simulates successful notification).
        /// </summary>
        /// <param name="tickCount">Timestamp to record as last successful notification.</param>
        internal static void RecordStatusNotificationForTesting(long tickCount)
        {
            lock (StatusNotifyGate)
            {
                _lastStatusNotifyTickCount = tickCount;
            }
        }

        /// <summary>
        /// Resets systemd status notification state for testing.
        /// </summary>
        internal static void ResetSystemdStatusNotificationStateForTesting()
        {
            lock (StatusNotifyGate)
            {
                _lastStatusNotifyTickCount = -1;
            }
        }

        /// <summary>
        /// Determines if the sd_notify result indicates success.
        /// </summary>
        /// <param name="result">sd_notify return value.</param>
        /// <returns><see langword="true"/> if the result indicates success; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// Per sd_notify(3), return values are: positive = sent, 0 = not sent (NOTIFY_SOCKET unset), negative = error.
        /// </remarks>
        private static bool IsSdNotifySuccess(int result)
        {
            return result > 0;
        }

        #endregion

        #region Native Interop

        /// <summary>
        /// Source-generated interop for sd_notify() from libsystemd.so.
        /// </summary>
        /// <param name="unsetEnvironment">If non-zero, unsets the NOTIFY_SOCKET environment variable after notification.</param>
        /// <param name="state">Notification state string (e.g., "READY=1").</param>
        /// <returns>Positive on success, negative on error, zero when not running under systemd.</returns>
        [LibraryImport("libsystemd.so.0", EntryPoint = "sd_notify", StringMarshalling = StringMarshalling.Utf8)]
        private static partial int SdNotify(int unsetEnvironment, string state);

        #endregion

        [LoggerMessage(EventId = 1300, Level = LogLevel.Debug, Message = "Notified systemd that service is ready")]
        /// <summary>
        /// Performs the log systemd ready notified operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdReadyNotified(ILogger logger);

        [LoggerMessage(EventId = 1301, Level = LogLevel.Debug, Message = "Notified systemd that service is stopping")]
        /// <summary>
        /// Performs the log systemd stopping notified operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdStoppingNotified(ILogger logger);

        [LoggerMessage(EventId = 1302, Level = LogLevel.Debug, Message = "Notified systemd with status: {Status}")]
        /// <summary>
        /// Performs the log systemd status notified operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdStatusNotified(ILogger logger, string status);

        [LoggerMessage(EventId = 1303, Level = LogLevel.Debug, Message = "systemd {NotificationType} notification failed with result {Result}")]
        /// <summary>
        /// Performs the log systemd notification failed result operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdNotificationFailedResult(ILogger logger, string notificationType, int result);

        [LoggerMessage(EventId = 1304, Level = LogLevel.Debug, Message = "systemd {NotificationType} notification skipped: libsystemd.so.0 not found")]
        /// <summary>
        /// Performs the log systemd library not found operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdLibraryNotFound(ILogger logger, string notificationType);

        [LoggerMessage(EventId = 1305, Level = LogLevel.Debug, Message = "systemd {NotificationType} notification skipped: sd_notify entry point not found")]
        /// <summary>
        /// Performs the log systemd entry point not found operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdEntryPointNotFound(ILogger logger, string notificationType);

        [LoggerMessage(EventId = 1306, Level = LogLevel.Debug, Message = "Exception during systemd {NotificationType} notification")]
        /// <summary>
        /// Performs the log systemd notification exception operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSystemdNotificationException(ILogger logger, Exception exception, string notificationType);
    }
}
