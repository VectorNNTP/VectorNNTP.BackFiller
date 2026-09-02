// <copyright file="OperationalCommandParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the operational command parser behavior.

using System.Collections.Frozen;

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Parses command-line tokens into at most one <see cref="OperationalCommand"/> and emits fail-closed parse
    /// diagnostics for invalid invocations.
    /// </summary>
    /// <remarks>
    /// Parsing failures are reported to standard error and surfaced as explicit exit codes rather than exceptions.
    /// This parser does not participate in startup validation logging; it only validates command-line shape.
    /// </remarks>
    internal static class OperationalCommandParser
    {
        /// <summary>
        /// Canonical mapping from accepted command-line tokens to operational command values.
        /// </summary>
        private static readonly FrozenDictionary<string, OperationalCommand> CommandMap =
            new Dictionary<string, OperationalCommand>(StringComparer.OrdinalIgnoreCase)
            {
                ["--help"] = OperationalCommand.Help,
                ["--version"] = OperationalCommand.Version,
                ["--validate-config"] = OperationalCommand.ValidateConfig,
                ["--validate-startup"] = OperationalCommand.ValidateStartup,
                ["--diagnostics"] = OperationalCommand.Diagnostics,
                ["--dump-config"] = OperationalCommand.DumpConfig,
            }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Parses command-line arguments and returns either a selected operational command or an explicit parse error exit code.
        /// </summary>
        /// <param name="args">Command-line arguments to inspect.</param>
        /// <param name="command">When this method returns, contains the parsed command, or <see langword="null"/> when no command was provided.</param>
        /// <param name="errorExitCode">When this method returns, contains the parse error exit code, or <see langword="null"/> when parsing succeeded.</param>
        /// <returns><see langword="true"/> when parsing succeeded; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// <para>Parses arguments left-to-right using a strict fail-closed contract.</para>
        /// <para>Unknown <c>--option</c> values, positional arguments, and multiple commands are rejected with exit code 2.</para>
        /// </remarks>
        internal static bool TryParseCommandLine(string[] args, out OperationalCommand? command, out int? errorExitCode)
        {
            ArgumentNullException.ThrowIfNull(args);

            command = null;
            errorExitCode = null;

            if (args.Length == 0)
            {
                return true;
            }

            foreach (string? rawArg in args)
            {
                if (string.IsNullOrWhiteSpace(rawArg))
                {
                    errorExitCode = HandleUnexpectedPositionalArgument(FormatInvalidArgument(rawArg));
                    command = null;
                    return false;
                }

                if (TryMapCommand(rawArg, out OperationalCommand parsedCommand))
                {
                    if (command.HasValue)
                    {
                        errorExitCode = HandleMultipleCommands(ToCommandToken(command.Value), rawArg);
                        command = null;
                        return false;
                    }

                    command = parsedCommand;
                    continue;
                }

                errorExitCode = rawArg.StartsWith("--", StringComparison.OrdinalIgnoreCase)
                    ? HandleUnknownOption(rawArg)
                    : HandleUnexpectedPositionalArgument(rawArg);
                command = null;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Attempts to map a raw argument token to a known operational command token.
        /// </summary>
        /// <param name="token">Raw argument token to classify.</param>
        /// <param name="command">When this method returns, contains the mapped command when mapping succeeds.</param>
        /// <returns><see langword="true"/> when the token is a supported command; otherwise <see langword="false"/>.</returns>
        /// <exception cref="ArgumentException"><paramref name="token"/> is <see langword="null"/>, empty, or whitespace.</exception>
        private static bool TryMapCommand(string token, out OperationalCommand command)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(token);
            return CommandMap.TryGetValue(token, out command);
        }

        /// <summary>
        /// Converts an operational command value to its canonical command-line token.
        /// </summary>
        /// <param name="command">Operational command value to format.</param>
        /// <returns>The canonical token (for example <c>--validate-config</c>) for <paramref name="command"/>.</returns>
        /// <exception cref="System.Diagnostics.UnreachableException">The <paramref name="command"/> value is not a supported enum member.</exception>
        private static string ToCommandToken(OperationalCommand command)
        {
            return command switch
            {
                OperationalCommand.Help => "--help",
                OperationalCommand.Version => "--version",
                OperationalCommand.ValidateConfig => "--validate-config",
                OperationalCommand.ValidateStartup => "--validate-startup",
                OperationalCommand.Diagnostics => "--diagnostics",
                OperationalCommand.DumpConfig => "--dump-config",
                _ => throw new System.Diagnostics.UnreachableException($"Unsupported command enum value: {command}")
            };
        }

        /// <summary>
        /// Emits parser diagnostics for an unknown <c>--option</c> token and returns the parse-failure exit code.
        /// </summary>
        /// <param name="option">Unknown option token as provided on the command line.</param>
        /// <returns><see cref="ExitCodePolicy.ExitCodeConfigurationFailure"/>.</returns>
        /// <exception cref="ArgumentException"><paramref name="option"/> is <see langword="null"/>, empty, or whitespace.</exception>
        private static int HandleUnknownOption(string option)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(option);

            Console.Error.WriteLine($"ERROR: Unknown option '{option}'.");
            Console.Error.WriteLine("Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Emits parser diagnostics when multiple command tokens are supplied in a single invocation.
        /// </summary>
        /// <param name="firstCommand">First recognized command token.</param>
        /// <param name="secondCommand">Second recognized command token that violates single-command parsing rules.</param>
        /// <returns><see cref="ExitCodePolicy.ExitCodeConfigurationFailure"/>.</returns>
        /// <exception cref="ArgumentException"><paramref name="firstCommand"/> or <paramref name="secondCommand"/> is <see langword="null"/>, empty, or whitespace.</exception>
        private static int HandleMultipleCommands(string firstCommand, string secondCommand)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(firstCommand);
            ArgumentException.ThrowIfNullOrWhiteSpace(secondCommand);

            Console.Error.WriteLine($"ERROR: Multiple commands specified: '{firstCommand}' and '{secondCommand}'.");
            Console.Error.WriteLine("Specify exactly one command. Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Emits parser diagnostics for an unexpected positional argument and returns the parse-failure exit code.
        /// </summary>
        /// <param name="argument">Unexpected non-command token to report.</param>
        /// <returns><see cref="ExitCodePolicy.ExitCodeConfigurationFailure"/>.</returns>
        /// <exception cref="ArgumentException"><paramref name="argument"/> is <see langword="null"/> or empty.</exception>
        private static int HandleUnexpectedPositionalArgument(string argument)
        {
            ArgumentException.ThrowIfNullOrEmpty(argument);

            Console.Error.WriteLine($"ERROR: Unexpected argument '{argument}'.");
            Console.Error.WriteLine("Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Formats null/empty/whitespace argument values into explicit sentinel text for parse diagnostics.
        /// </summary>
        /// <param name="argument">Raw argument value to normalize for error output.</param>
        /// <returns>A printable sentinel token such as <c>&lt;null&gt;</c>, <c>&lt;empty&gt;</c>, or <c>&lt;whitespace&gt;</c>.</returns>
        private static string FormatInvalidArgument(string? argument)
        {
            return argument switch
            {
                null => "<null>",
                "" => "<empty>",
                _ when string.IsNullOrWhiteSpace(argument) => "<whitespace>",
                _ => argument
            };
        }
    }
}
