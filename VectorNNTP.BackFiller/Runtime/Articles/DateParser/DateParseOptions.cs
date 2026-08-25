// <copyright file="DateParseOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Immutable parser options controlling date-header guardrails and normalization behavior.

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Optional behavior for date header parsing and canonicalization.
    /// </summary>
    /// <param name="MaxInputLength">Maximum characters accepted for one date value.</param>
    /// <param name="RequireKnownTimezoneAbbreviation">Indicates whether unknown trailing timezone abbreviations are rejected.</param>
    /// <param name="NormalizeInteriorWhitespace">Indicates whether consecutive interior spaces are collapsed before parse.</param>
    internal readonly record struct DateParseOptions(
        int MaxInputLength,
        bool RequireKnownTimezoneAbbreviation,
        bool NormalizeInteriorWhitespace)
    {
        /// <summary>
        /// Gets default options for article date parsing.
        /// </summary>
        internal static DateParseOptions Default => new(
            MaxInputLength: 512,
            RequireKnownTimezoneAbbreviation: false,
            NormalizeInteriorWhitespace: true);
    }
}
