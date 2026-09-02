// <copyright file="HelpCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the help command handler behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Handles the <c>--help</c> operational command by emitting usage guidance and exit-code semantics.
    /// </summary>
    /// <remarks>
    /// This command is informational only and does not perform configuration validation or startup dependency checks.
    /// It writes static help text to standard output and leaves validation/error logging responsibilities to other
    /// command paths.
    /// </remarks>
    internal static class HelpCommandHandler
    {
        /// <summary>
        /// Writes the built-in command reference, examples, deployment usage notes, and exit-code table to standard output.
        /// </summary>
        /// <returns><see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> after the help payload is emitted.</returns>
        internal static int Handle()
        {
            Console.WriteLine(@"
VectorNNTP.BackFiller — NNTP Article Backfiller Service

USAGE:
  VectorNNTP.BackFiller [options]

OPTIONS:
  --help                  Display this help message and exit (code: 0)
  --version               Display version and build information (code: 0)
  --validate-config       Validate configuration only (0 success, 2 invalid config)
  --validate-startup      Validate configuration + dependencies (0 success, 2 config error, 3 dependency error)
  --diagnostics           Display startup diagnostics and exit (code: 0)
  --dump-config           Display constrained safe configuration view and exit (code: 0)
  (none)                  Start the service normally

EXAMPLES:
  # Start service in foreground
  VectorNNTP.BackFiller

  # Check service version
  VectorNNTP.BackFiller --version

  # Validate configuration structure and semantics
  VectorNNTP.BackFiller --validate-config

  # Validate startup dependencies (database, Cloudflare)
  VectorNNTP.BackFiller --validate-startup

  # Display configuration for verification
  VectorNNTP.BackFiller --dump-config

  # Get startup diagnostics
  VectorNNTP.BackFiller --diagnostics

DEPLOYMENT USAGE:
  # systemd: Run startup validation before starting service
  ExecStartPre=/usr/bin/dotnet /opt/vectornntp/VectorNNTP.BackFiller.dll --validate-startup

  # Docker: Print logs before container entrypoint
  RUN dotnet /app/VectorNNTP.BackFiller.dll --diagnostics

  # Kubernetes: In init container to verify startup readiness
  initContainers:
  - name: startup-validator
    command: [
      ""dotnet"", ""/app/VectorNNTP.BackFiller.dll"",
      ""--validate-startup""
    ]

EXIT CODES:
  0   Command succeeded or normal shutdown
  1   Unexpected error
  2   Invalid command line or configuration validation failure
  3   Dependency validation failure
  4   Storage failure
  5   Startup failure
");
            return ExitCodePolicy.ExitCodeNormalShutdown;
        }
    }
}
