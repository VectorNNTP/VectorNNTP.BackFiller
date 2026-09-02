// <copyright file="ValidationResults.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the validation results behavior.

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Captures the aggregated outcome of one configuration-validation pass, including all collected errors and warnings.
    /// </summary>
    /// <remarks>
    /// Instances are created after validators finish accumulating messages so startup and command handlers can make
    /// a single validity decision and still report every issue found in that pass.
    /// </remarks>
    internal sealed class ConfigurationValidationResult
    {
        /// <summary>
        /// Indicates whether this validation pass produced zero configuration errors.
        /// </summary>
        /// <value><see langword="true"/> when <see cref="Errors"/> is empty; otherwise, <see langword="false"/>.</value>
        public bool IsValid { get; }

        /// <summary>
        /// Ordered configuration errors captured during validation.
        /// </summary>
        /// <value>
        /// A read-only snapshot of <c>(Setting, Error)</c> pairs copied at construction time.
        /// Callers cannot mutate this collection through the returned interface.
        /// </value>
        public IReadOnlyList<(string Setting, string Error)> Errors { get; }

        /// <summary>
        /// Ordered non-fatal configuration findings captured during validation.
        /// </summary>
        /// <value>
        /// A read-only snapshot of <c>(Setting, Message)</c> pairs copied at construction time.
        /// Warnings do not change <see cref="IsValid"/>.
        /// </value>
        public IReadOnlyList<(string Setting, string Message)> Warnings { get; }

        /// <summary>
        /// Creates a configuration-validation outcome from accumulated error and warning sequences.
        /// </summary>
        /// <param name="errors">
        /// Error pairs copied into the <see cref="Errors"/> snapshot; <see langword="null"/> is treated as an empty sequence.
        /// </param>
        /// <param name="warnings">
        /// Warning pairs copied into the <see cref="Warnings"/> snapshot; <see langword="null"/> is treated as an empty sequence.
        /// </param>
        /// <remarks>
        /// Input sequences are materialized immediately, preserving enumeration order and decoupling the result from
        /// subsequent caller-side collection mutations.
        /// </remarks>
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
        /// Creates a configuration-validation result with no errors and no warnings.
        /// </summary>
        /// <returns>A new valid result instance whose message collections are empty snapshots.</returns>
        public static ConfigurationValidationResult Success()
        {
            return new ConfigurationValidationResult([], []);
        }
    }

    /// <summary>
    /// Captures the aggregated outcome of startup dependency validation, including hard failures, warnings, and errors.
    /// </summary>
    /// <remarks>
    /// Dependency probe runners compose multiple probe outputs into this type so startup can distinguish blocking
    /// dependency problems from non-blocking warnings while retaining full diagnostic detail.
    /// </remarks>
    internal sealed class DependencyValidationResult
    {
        /// <summary>
        /// Indicates whether dependency validation produced no failed dependencies and no dependency errors.
        /// </summary>
        /// <value>
        /// <see langword="true"/> only when both <see cref="FailedDependencies"/> and <see cref="Errors"/> are empty.
        /// Warnings alone do not make the result invalid.
        /// </value>
        public bool IsValid => FailedDependencies.Count == 0 && Errors.Count == 0;

        /// <summary>
        /// Ordered list of dependencies that failed validation and the associated failure reasons.
        /// </summary>
        /// <value>
        /// A read-only snapshot of <c>(Dependency, Reason)</c> pairs copied at construction time.
        /// </value>
        public IReadOnlyList<(string Dependency, string Reason)> FailedDependencies { get; }

        /// <summary>
        /// Ordered non-fatal dependency diagnostics.
        /// </summary>
        /// <value>
        /// A read-only snapshot of <c>(Category, Message)</c> pairs copied at construction time.
        /// </value>
        public IReadOnlyList<(string Category, string Message)> Warnings { get; }

        /// <summary>
        /// Ordered dependency diagnostics treated as errors for startup validity.
        /// </summary>
        /// <value>
        /// A read-only snapshot of <c>(Category, Message)</c> pairs copied at construction time.
        /// </value>
        public IReadOnlyList<(string Category, string Message)> Errors { get; }

        /// <summary>
        /// Creates a dependency-validation outcome from aggregated probe diagnostics.
        /// </summary>
        /// <param name="failures">
        /// Failed dependency pairs copied into <see cref="FailedDependencies"/>; <see langword="null"/> is treated as empty.
        /// </param>
        /// <param name="warnings">
        /// Non-fatal diagnostic pairs copied into <see cref="Warnings"/>; <see langword="null"/> is treated as empty.
        /// </param>
        /// <param name="errors">
        /// Error diagnostic pairs copied into <see cref="Errors"/>; <see langword="null"/> is treated as empty.
        /// </param>
        /// <remarks>
        /// Input sequences are materialized immediately in enumeration order and exposed as read-only snapshots.
        /// </remarks>
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
        /// Creates a dependency-validation result with no failures, warnings, or errors.
        /// </summary>
        /// <returns>A new valid result instance containing empty diagnostic snapshots.</returns>
        public static DependencyValidationResult Success()
        {
            return new DependencyValidationResult([], [], []);
        }
    }
}
