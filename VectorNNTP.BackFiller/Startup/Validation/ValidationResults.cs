// <copyright file="ValidationResults.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the validation results behavior.

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Result of validating configuration options.
    /// </summary>
    internal sealed class ConfigurationValidationResult
    {
        /// <summary>
        /// Gets a value indicating whether all configuration is valid.
        /// </summary>
        public bool IsValid { get; }

        /// <summary>
        /// Returns the collection of validation errors (name, message pairs).
        /// </summary>
        public IReadOnlyList<(string Setting, string Error)> Errors { get; }

        /// <summary>
        /// Returns the collection of validation warnings (name, message pairs).
        /// </summary>
        public IReadOnlyList<(string Setting, string Message)> Warnings { get; }

        /// <summary>
        /// Creates a validation result with the given errors and warnings.
        /// </summary>
        public ConfigurationValidationResult(
            IEnumerable<(string, string)> errors,
            IEnumerable<(string, string)>? warnings = null)
        {
            List<(string, string)> errorList = errors?.ToList() ?? [];
            List<(string, string)> warningList = warnings?.ToList() ?? [];
            Errors = errorList.AsReadOnly();
            Warnings = warningList.AsReadOnly();
            IsValid = errorList.Count == 0;
        }

        /// <summary>
        /// Creates a successful validation result.
        /// </summary>
        public static ConfigurationValidationResult Success()
        {
            return new ConfigurationValidationResult([], []);
        }
    }

    /// <summary>
    /// Result of validating dependency connectivity.
    /// </summary>
    internal sealed class DependencyValidationResult
    {
        /// <summary>
        /// Gets a value indicating whether all dependency validation concerns succeeded.
        /// </summary>
        public bool IsValid => FailedDependencies.Count == 0 && Errors.Count == 0;

        /// <summary>
        /// Returns the collection of failed dependencies (name, reason pairs).
        /// </summary>
        public IReadOnlyList<(string Dependency, string Reason)> FailedDependencies { get; }

        /// <summary>
        /// Returns the collection of warnings.
        /// </summary>
        public IReadOnlyList<(string Category, string Message)> Warnings { get; }

        /// <summary>
        /// Returns the collection of errors.
        /// </summary>
        public IReadOnlyList<(string Category, string Message)> Errors { get; }

        /// <summary>
        /// Creates a dependency validation result.
        /// </summary>
        /// <param name="failures">Dependency failures.</param>
        /// <param name="warnings">Supplemental warnings.</param>
        /// <param name="errors">Supplemental errors.</param>
        public DependencyValidationResult(
            IEnumerable<(string Dependency, string Reason)>? failures,
            IEnumerable<(string Category, string Message)>? warnings,
            IEnumerable<(string Category, string Message)>? errors)
        {
            List<(string Dependency, string Reason)> failureList = failures?.ToList() ?? [];
            List<(string Category, string Message)> warningList = warnings?.ToList() ?? [];
            List<(string Category, string Message)> errorList = errors?.ToList() ?? [];

            FailedDependencies = failureList.AsReadOnly();
            Warnings = warningList.AsReadOnly();
            Errors = errorList.AsReadOnly();
        }

        /// <summary>
        /// Creates a successful dependency validation result.
        /// </summary>
        public static DependencyValidationResult Success()
        {
            return new DependencyValidationResult([], [], []);
        }
    }
}
