// <copyright file="CloudflareDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the cloudflare dependency probe behavior.

using CloudFlare.Client;
using CloudFlare.Client.Api.Result;
using CloudFlare.Client.Api.Zones;
using CloudFlare.Client.Contexts;
using CloudFlare.Client.Enumerators;
using Serilog;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Executes startup Cloudflare zone validation and converts API/provider outcomes into sanitized dependency diagnostics.
    /// </summary>
    /// <remarks>
    /// This probe validates that configured zone identity is reachable and consistent with BackFiller DNS identity settings.
    /// It reports outcomes via <see cref="DependencyValidationResult"/> for aggregation by the dependency runner rather than
    /// deciding startup exit behavior directly.
    /// </remarks>
    internal static class CloudflareDependencyProbe
    {
        /// <summary>
        /// Validates Cloudflare zone reachability, active status, and DNS-suffix consistency for startup dependency checks.
        /// </summary>
        /// <remarks>
        /// <para>Performs a control-plane API call using the configured Cloudflare token and zone identifier.</para>
        /// <para>The configured <c>BackFiller:DnsSuffix</c> must canonicalize to the same name as the resolved Cloudflare zone.</para>
        /// <para>Token values are intentionally excluded from logs and returned diagnostics.</para>
        /// </remarks>
        /// <param name="backFiller">Validated BackFiller options that carry DNS suffix and optional Cloudflare zone/token settings.</param>
        /// <param name="timeout">Maximum duration allowed for Cloudflare zone verification.</param>
        /// <param name="cancellationToken">Startup cancellation token propagated to Cloudflare API calls.</param>
        /// <returns>
        /// A task that completes with a <see cref="DependencyValidationResult"/>. Missing Cloudflare settings produce an
        /// empty result, and verification failures are returned as sanitized <c>CloudflareZone</c> dependency failures.
        /// </returns>
        /// <exception cref="OperationCanceledException">The outer <paramref name="cancellationToken"/> is canceled.</exception>
        internal static async Task<DependencyValidationResult> ValidateCloudflareZoneDependencyAsync(
            BackFillerOptions? backFiller,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            string? dnsSuffix = backFiller?.DnsSuffix;
            string? zoneId = backFiller?.LetsEncrypt?.CloudFlareZoneId;
            string? apiToken = backFiller?.LetsEncrypt?.CloudFlareApiToken;

            if (string.IsNullOrWhiteSpace(dnsSuffix) ||
                string.IsNullOrWhiteSpace(zoneId) ||
                string.IsNullOrWhiteSpace(apiToken))
            {
                // Configuration validation should catch this, but guard defensively.
                return new DependencyValidationResult(failures, warnings, errors);
            }

            string expectedZoneName = BackFillerIdentityValidator.CanonicalizeDnsSuffix(dnsSuffix);

            try
            {
                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

                using CloudFlareClient client = new(apiToken.Trim(), new ConnectionInfo());
                CloudFlareResult<Zone> zoneResult = await client.Zones.GetDetailsAsync(zoneId.Trim(), cts.Token).ConfigureAwait(false);

                if (!zoneResult.Success || zoneResult.Result == null)
                {
                    string sanitizedFailureReason = GetSanitizedCloudflareApiFailureReason(zoneResult.Errors.Select(static x => x.Message));
                    failures.Add(("CloudflareZone", $"Zone verification failed for configured ZoneId: {sanitizedFailureReason}"));
                    return new DependencyValidationResult(failures, warnings, errors);
                }

                if (zoneResult.Result.Status != ZoneStatus.Active)
                {
                    failures.Add((
                        "CloudflareZone",
                        $"Configured Cloudflare zone is not active (status: {zoneResult.Result.Status})"));
                }

                string actualZoneName = zoneResult.Result.Name?.Trim().TrimEnd('.').ToLowerInvariant() ?? string.Empty;
                if (!string.Equals(actualZoneName, expectedZoneName, StringComparison.Ordinal))
                {
                    failures.Add((
                        "CloudflareZone",
                        $"Configured BackFiller:DnsSuffix '{expectedZoneName}' does not match Cloudflare zone name '{actualZoneName}' for the configured CloudFlareZoneId"));
                }

                if (failures.Count == 0)
                {
                    Log.Information(
                        "Cloudflare zone verification validated successfully (ZoneId: {ZoneId}, Zone: {ZoneName}, DnsSuffix: {DnsSuffix})",
                        zoneResult.Result.Id,
                        actualZoneName,
                        expectedZoneName);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                failures.Add(("CloudflareZone", $"Cloudflare zone verification timed out after {timeout.TotalSeconds:F1}s"));
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "Cloudflare zone verification threw an exception during startup dependency validation.");
                string sanitizedFailureReason = GetSanitizedCloudflareExceptionFailureReason(ex);
                failures.Add(("CloudflareZone", $"Cloudflare zone verification failed: {sanitizedFailureReason}"));
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }

        /// <summary>
        /// Maps Cloudflare API error payload text to bounded sanitized startup-diagnostic categories.
        /// </summary>
        /// <param name="providerMessages">Provider-reported Cloudflare error messages.</param>
        /// <returns>A controlled failure reason suitable for dependency-validation output.</returns>
        /// <remarks>
        /// Message content is normalized and pattern-matched into authentication, authorization, zone-not-found,
        /// or generic API-request-failed categories.
        /// </remarks>
        private static string GetSanitizedCloudflareApiFailureReason(IEnumerable<string> providerMessages)
        {
            ArgumentNullException.ThrowIfNull(providerMessages);

            string combinedMessage = string.Join("; ", providerMessages.Where(static x => !string.IsNullOrWhiteSpace(x))).ToLowerInvariant();
            return string.IsNullOrWhiteSpace(combinedMessage)
                ? "Invalid response"
                : combinedMessage.Contains("unauthorized", StringComparison.Ordinal) ||
                combinedMessage.Contains("authentication", StringComparison.Ordinal) ||
                combinedMessage.Contains("api token", StringComparison.Ordinal) ||
                combinedMessage.Contains("invalid token", StringComparison.Ordinal)
                ? "Authentication failed"
                : combinedMessage.Contains("forbidden", StringComparison.Ordinal) ||
                combinedMessage.Contains("access denied", StringComparison.Ordinal) ||
                combinedMessage.Contains("permission", StringComparison.Ordinal)
                ? "Access denied"
                : combinedMessage.Contains("not found", StringComparison.Ordinal) ||
                combinedMessage.Contains("zone not found", StringComparison.Ordinal) ||
                combinedMessage.Contains("invalid zone", StringComparison.Ordinal)
                ? "Zone not found"
                : "API request failed";
        }

        /// <summary>
        /// Maps Cloudflare transport/provider exceptions to bounded sanitized failure categories.
        /// </summary>
        /// <param name="ex">Exception observed during Cloudflare dependency validation.</param>
        /// <returns>A controlled failure reason suitable for dependency-validation output.</returns>
        /// <remarks>
        /// HTTP transport exceptions map directly to generic API-failure diagnostics; other exception messages are
        /// normalized and categorized using the same authentication/authorization/zone-not-found policy.
        /// </remarks>
        private static string GetSanitizedCloudflareExceptionFailureReason(Exception ex)
        {
            ArgumentNullException.ThrowIfNull(ex);

            if (ex is HttpRequestException)
            {
                return "API request failed";
            }

            string message = ex.Message?.Trim().ToLowerInvariant() ?? string.Empty;
            return message.Contains("unauthorized", StringComparison.Ordinal) ||
                message.Contains("authentication", StringComparison.Ordinal) ||
                message.Contains("api token", StringComparison.Ordinal) ||
                message.Contains("invalid token", StringComparison.Ordinal)
                ? "Authentication failed"
                : message.Contains("forbidden", StringComparison.Ordinal) ||
                message.Contains("access denied", StringComparison.Ordinal) ||
                message.Contains("permission", StringComparison.Ordinal)
                ? "Access denied"
                : message.Contains("not found", StringComparison.Ordinal) ||
                message.Contains("zone not found", StringComparison.Ordinal) ||
                message.Contains("invalid zone", StringComparison.Ordinal)
                ? "Zone not found"
                : "API request failed";
        }
    }
}
