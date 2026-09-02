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
    /// Owns command-line parsing for operational commands.
    /// </summary>
    internal static class OperationalCommandParser
    {
        /// <summary>
        /// Stores command map used by operational command parser.
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
        /// Attempts to map a raw argument token to an operational command.
        /// </summary>
        private static bool TryMapCommand(string token, out OperationalCommand command)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(token);
            return CommandMap.TryGetValue(token, out command);
        }

        /// <summary>
        /// Converts an operational command value to its canonical command-line token.
        /// </summary>
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
        /// Handles an unknown command-line option and returns a parsing error code.
        /// </summary>
        /// <param name="option">The unknown option value.</param>
        private static int HandleUnknownOption(string option)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(option);

            Console.Error.WriteLine($"ERROR: Unknown option '{option}'.");
            Console.Error.WriteLine("Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Handles multiple commands specified in one invocation.
        /// </summary>
        /// <param name="firstCommand">The first command detected.</param>
        /// <param name="secondCommand">The second command detected.</param>
        private static int HandleMultipleCommands(string firstCommand, string secondCommand)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(firstCommand);
            ArgumentException.ThrowIfNullOrWhiteSpace(secondCommand);

            Console.Error.WriteLine($"ERROR: Multiple commands specified: '{firstCommand}' and '{secondCommand}'.");
            Console.Error.WriteLine("Specify exactly one command. Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Handles unexpected positional arguments.
        /// </summary>
        /// <param name="argument">The positional argument value.</param>
        private static int HandleUnexpectedPositionalArgument(string argument)
        {
            ArgumentException.ThrowIfNullOrEmpty(argument);

            Console.Error.WriteLine($"ERROR: Unexpected argument '{argument}'.");
            Console.Error.WriteLine("Use --help to see supported commands.");
            return ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        /// <summary>
        /// Formats invalid argument values for diagnostic output.
        /// </summary>
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
