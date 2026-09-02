// <copyright file="DumpConfigCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the dump config command handler responsibilities for this subsystem boundary.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns the dump-config operational command behavior.
    /// </summary>
    internal static class DumpConfigCommandHandler
    {
        /// <summary>
        /// Stores the dump config included section prefixes state used to enforce this component's runtime contract.
        /// </summary>
        private static readonly string[] DumpConfigIncludedSectionPrefixes =
        [
            "BackFiller",
            "ConnectionStrings"
        ];

        /// <summary>
        /// Stores the dump config clear text keys state used to enforce this component's runtime contract.
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
        /// Displays a constrained configuration view with conservative secret redaction.
        /// </summary>
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
        /// Dumps selected configuration keys with allowlist-based cleartext output.
        /// </summary>
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
        /// Determines whether a key belongs to the constrained diagnostic dump scope.
        /// </summary>
        private static bool ShouldIncludeInDump(string key)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return DumpConfigIncludedSectionPrefixes.Any(prefix =>
                key.Equals(prefix, StringComparison.OrdinalIgnoreCase) ||
                key.StartsWith(prefix + ":", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Resolves a safe display value for a configuration entry.
        /// </summary>
        private static string GetDumpDisplayValue(string key, string? value)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return string.IsNullOrEmpty(value) ? "[EMPTY]" : DumpConfigClearTextKeys.Contains(key) ? value : "[REDACTED]";
        }
    }
}
