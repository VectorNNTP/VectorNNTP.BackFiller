// <copyright file="DumpConfigCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the dump config command handler behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Handles the <c>--dump-config</c> operational command by rendering a constrained, redacted configuration view.
    /// </summary>
    /// <remarks>
    /// This handler is diagnostic output for operators and does not execute startup validation probes. Secret handling
    /// is allowlist-based: only explicitly approved keys are shown in clear text while all other included values are
    /// redacted before output.
    /// </remarks>
    internal static class DumpConfigCommandHandler
    {
        /// <summary>
        /// Configuration section prefixes eligible for inclusion in dump output.
        /// </summary>
        private static readonly string[] DumpConfigIncludedSectionPrefixes =
        [
            "BackFiller",
            "ConnectionStrings"
        ];

        /// <summary>
        /// Exact configuration keys allowed to be emitted in clear text within dump output.
        /// </summary>
        private static readonly HashSet<string> DumpConfigClearTextKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "BackFiller:Name",
            "BackFiller:Id",
            "BackFiller:DnsSuffix",
            "BackFiller:BindPort",
            "BackFiller:LetsEncrypt:Enabled",
            "BackFiller:LetsEncrypt:UseStagingDirectory"
        };

        /// <summary>
        /// Writes the constrained configuration dump and redaction policy banner to console output.
        /// </summary>
        /// <param name="configuration">The configuration root to inspect for dump output.</param>
        /// <returns>
        /// <see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> when dump output completes;
        /// otherwise <see cref="ExitCodePolicy.ExitCodeUnexpectedFailure"/> when configuration is unavailable.
        /// </returns>
        internal static int Handle(IConfiguration? configuration)
        {
            if (configuration == null)
            {
                Console.Error.WriteLine("ERROR: Configuration not available");
                return ExitCodePolicy.ExitCodeUnexpectedFailure;
            }

            Console.WriteLine("Current Configuration (safe view):\n");
            Console.WriteLine("Only BackFiller and ConnectionStrings settings are shown.");
            Console.WriteLine("Only explicitly allowlisted non-secret values are displayed in cleartext; all other displayed values are redacted.\n");

            DumpConfigSection(configuration);

            return ExitCodePolicy.ExitCodeNormalShutdown;
        }

        /// <summary>
        /// Emits included configuration keys in deterministic key order with clear-text or redacted values.
        /// </summary>
        /// <param name="config">Configuration root to enumerate.</param>
        /// <remarks>
        /// Only non-null configuration entries are considered. Inclusion is restricted by
        /// <see cref="DumpConfigIncludedSectionPrefixes"/>, and value rendering is delegated to
        /// <see cref="GetDumpDisplayValue(string, string?)"/>.
        /// </remarks>
        private static void DumpConfigSection(IConfiguration config)
        {
            foreach (KeyValuePair<string, string?> item in config.AsEnumerable().Where(static x => x.Value != null).OrderBy(static x => x.Key, StringComparer.OrdinalIgnoreCase))
            {
                if (!ShouldIncludeInDump(item.Key))
                {
                    continue;
                }

                string displayValue = GetDumpDisplayValue(item.Key, item.Value);
                Console.WriteLine($"{item.Key}: {displayValue}");
            }
        }

        /// <summary>
        /// Determines whether a key falls within the allowed dump sections.
        /// </summary>
        /// <param name="key">Configuration key to evaluate.</param>
        /// <returns>
        /// <see langword="true"/> when the key exactly matches, or is nested under, an allowlisted section prefix;
        /// otherwise <see langword="false"/>.
        /// </returns>
        /// <exception cref="ArgumentException"><paramref name="key"/> is <see langword="null"/>, empty, or whitespace.</exception>
        private static bool ShouldIncludeInDump(string key)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return DumpConfigIncludedSectionPrefixes.Any(prefix =>
                key.Equals(prefix, StringComparison.OrdinalIgnoreCase) ||
                key.StartsWith(prefix + ":", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Resolves the printed value token for an included configuration entry.
        /// </summary>
        /// <param name="key">Included configuration key being rendered.</param>
        /// <param name="value">Raw configuration value for <paramref name="key"/>.</param>
        /// <returns>
        /// <c>[EMPTY]</c> when the value is null/empty, the raw value for clear-text allowlisted keys, or
        /// <c>[REDACTED]</c> for all other included keys.
        /// </returns>
        /// <exception cref="ArgumentException"><paramref name="key"/> is <see langword="null"/>, empty, or whitespace.</exception>
        private static string GetDumpDisplayValue(string key, string? value)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return string.IsNullOrEmpty(value) ? "[EMPTY]" : DumpConfigClearTextKeys.Contains(key) ? value : "[REDACTED]";
        }
    }
}
