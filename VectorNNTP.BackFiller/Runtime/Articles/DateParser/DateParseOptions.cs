// <copyright file="DateParseOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Immutable parser options controlling date-header guardrails and normalization behavior.

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Controls the guardrails and normalization steps applied before the date parser attempts canonicalization.
    /// </summary>
    /// <param name="MaxInputLength">Maximum number of trimmed characters accepted for one candidate date header value.</param>
    /// <param name="RequireKnownTimezoneAbbreviation">Whether a trailing alphabetic timezone token must be recognized before exact parsing is attempted.</param>
    /// <param name="NormalizeInteriorWhitespace">Whether runs of interior ASCII spaces are collapsed before exact parsing.</param>
    internal readonly record struct DateParseOptions(
        int MaxInputLength,
        bool RequireKnownTimezoneAbbreviation,
        bool NormalizeInteriorWhitespace)
    {
        /// <summary>
        /// Gets the repository default date-parse options.
        /// </summary>
        /// <value>A configuration that accepts up to 512 characters, tolerates unknown timezone abbreviations, and normalizes repeated interior spaces.</value>
        internal static DateParseOptions Default => new(
            MaxInputLength: 512,
            RequireKnownTimezoneAbbreviation: false,
            NormalizeInteriorWhitespace: true);
    }
}
