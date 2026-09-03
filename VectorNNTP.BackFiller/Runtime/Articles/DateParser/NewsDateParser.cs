// <copyright file="NewsDateParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Allocation-conscious date parser for NNTP date header values with canonical UTC output.

using System.Collections.Frozen;
using System.Globalization;
using System.Text.RegularExpressions;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Parses raw article date-header values into the repository's canonical UTC RFC 5322 representation.
    /// </summary>
    /// <remarks>
    /// The parser trims input, enforces printable-ASCII and length guardrails, tries a fast invariant parse first, then falls back to curated cleanup and exact-format parsing with optional timezone-abbreviation validation.
    /// </remarks>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Exact-format parse styles used by DateTimeOffset parsing.
        /// </summary>
        private const DateTimeStyles ParseStyles = DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

        /// <summary>
        /// Canonical format pattern for UTC NNTP date values.
        /// </summary>
        private const string CanonicalFormat = "ddd, dd MMM yyyy HH:mm:ss";

        /// <summary>
        /// Curated exact parse formats accepted by the parser.
        /// </summary>
        private static readonly string[] DateFormats =
        [
            "ddd, dd MMM yyyy HH:mm:ss zzz",
            "ddd, d MMM yyyy HH:mm:ss zzz",
            "ddd, dd MMM yyyy H:mm:ss zzz",
            "ddd, d MMM yyyy H:mm:ss zzz",
            "ddd, dd MMM yyyy HH:mm:ss",
            "ddd, d MMM yyyy HH:mm:ss",
            "ddd, dd MMM yyyy H:mm:ss",
            "ddd, d MMM yyyy H:mm:ss",
            "dd MMM yyyy HH:mm:ss zzz",
            "d MMM yyyy HH:mm:ss zzz",
            "dd MMM yyyy H:mm:ss zzz",
            "d MMM yyyy H:mm:ss zzz",
            "dd MMM yyyy HH:mm:ss",
            "d MMM yyyy HH:mm:ss",
            "dd MMM yyyy H:mm:ss",
            "d MMM yyyy H:mm:ss",
            "yyyy-MM-dd HH:mm:ss zzz",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd H:mm:ss",
            "yyyy-MM-ddTHH:mm:ss zzz",
            "yyyy-MM-ddTHH:mm:sszzz",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy/MM/dd H:mm:ss",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy H:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy H:mm:ss",
            "dd MMM yyyy",
            "d MMM yyyy",
            "yyyy-MM-dd",
            "dd/MM/yyyy",
            "MM/dd/yyyy",
            "ddd, dd MMM yy HH:mm:ss zzz",
            "ddd, d MMM yy HH:mm:ss zzz",
            "ddd, dd MMM yy H:mm:ss zzz",
            "ddd, d MMM yy H:mm:ss zzz",
            "ddd, dd MMM yy HH:mm:ss",
            "ddd, d MMM yy HH:mm:ss",
            "ddd, dd MMM yy H:mm:ss",
            "ddd, d MMM yy H:mm:ss",
            "dd MMM yy HH:mm:ss zzz",
            "d MMM yy HH:mm:ss zzz",
            "dd MMM yy H:mm:ss zzz",
            "d MMM yy H:mm:ss zzz",
            "dd MMM yy HH:mm:ss",
            "d MMM yy HH:mm:ss",
            "dd MMM yy H:mm:ss",
            "d MMM yy H:mm:ss",
        ];

        /// <summary>
        /// Cached regex matching trailing timezone abbreviations.
        /// </summary>
        private static readonly Regex CachedTimezoneAbbrRegex = new(@"\s([A-Za-z]{2,8})$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

        /// <summary>
        /// Cached regex matching trailing parenthesized suffix comments.
        /// </summary>
        private static readonly Regex CachedParenthesisedTzRegex = new(@"\s*\([^)]*\)\s*$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

        /// <summary>
        /// Frozen timezone abbreviation mapping table.
        /// </summary>
        private static readonly FrozenDictionary<string, string> TimezoneMappings = CreateDefaultTimezoneMappings();

        /// <summary>
        /// Tries to canonicalize an NNTP date value using the repository default parse options.
        /// </summary>
        /// <param name="input">Raw date value.</param>
        /// <param name="canonicalValue">Canonical UTC date string ending in <c>+0000</c> when parsing succeeds.</param>
        /// <param name="failure">Failure reason when parsing fails.</param>
        /// <returns><see langword="true"/> when <paramref name="input"/> could be normalized into canonical UTC form.</returns>
        internal static bool TryGetCanonicalDateValue(ReadOnlySpan<char> input, out string canonicalValue, out DateParseFailureReason failure)
        {
            return TryGetCanonicalDateValue(input, DateParseOptions.Default, out canonicalValue, out failure);
        }

        /// <summary>
        /// Tries to canonicalize an NNTP date value using explicit normalization options.
        /// </summary>
        /// <param name="input">Raw candidate date value.</param>
        /// <param name="options">Guardrails and normalization toggles applied before exact parsing.</param>
        /// <param name="canonicalValue">Canonical UTC date string ending in <c>+0000</c> when parsing succeeds.</param>
        /// <param name="failure">Failure reason describing the stage that rejected <paramref name="input"/>.</param>
        /// <returns><see langword="true"/> when parsing succeeds.</returns>
        /// <remarks>
        /// The method trims the input, enforces <see cref="PrintableAsciiSimd.IsAllPrintableAscii(ReadOnlySpan{char})"/>, optionally collapses repeated interior spaces, optionally rejects unknown trailing timezone abbreviations, and finally formats successful results with <see cref="FormatCanonicalRfc5322Utc"/>.
        /// </remarks>
        internal static bool TryGetCanonicalDateValue(
            ReadOnlySpan<char> input,
            DateParseOptions options,
            out string canonicalValue,
            out DateParseFailureReason failure)
        {
            canonicalValue = string.Empty;
            failure = DateParseFailureReason.None;

            ReadOnlySpan<char> trimmed = input.Trim();
            if (trimmed.IsEmpty)
            {
                failure = DateParseFailureReason.Empty;
                return false;
            }

            if (trimmed.Length > options.MaxInputLength)
            {
                failure = DateParseFailureReason.TooLong;
                return false;
            }

            if (!PrintableAsciiSimd.IsAllPrintableAscii(trimmed))
            {
                failure = DateParseFailureReason.NonPrintableAscii;
                return false;
            }

            if (TryQuickParse(trimmed, out DateTime quickUtc))
            {
                canonicalValue = FormatCanonicalRfc5322Utc(quickUtc);
                return true;
            }

            string cleaned = trimmed.ToString();
            cleaned = CachedParenthesisedTzRegex.Replace(cleaned, string.Empty).Trim();
            if (options.NormalizeInteriorWhitespace)
            {
                cleaned = CollapseInteriorWhitespace(cleaned);
            }

            if (options.RequireKnownTimezoneAbbreviation && TryGetUnknownTrailingAbbreviation(cleaned, out _))
            {
                failure = DateParseFailureReason.UnknownTimezoneAbbreviation;
                return false;
            }

            cleaned = SubstituteTimezoneAbbreviation(cleaned);
            if (TryExactParse(cleaned, out DateTime exactUtc))
            {
                canonicalValue = FormatCanonicalRfc5322Utc(exactUtc);
                return true;
            }

            failure = DateParseFailureReason.ParseFailed;
            return false;
        }

        /// <summary>
        /// Formats a date-time value as the canonical UTC RFC 5322 string used by the parser.
        /// </summary>
        /// <param name="utc">Date-time value that should be interpreted as UTC output.</param>
        /// <returns>A canonical string in the form <c>ddd, dd MMM yyyy HH:mm:ss +0000</c>.</returns>
        /// <remarks>
        /// <paramref name="utc"/> is normalized to UTC before formatting: local values are converted with <see cref="DateTime.ToUniversalTime"/>, and unspecified values are treated as already UTC.
        /// </remarks>
        internal static string FormatCanonicalRfc5322Utc(DateTime utc)
        {
            if (utc.Kind == DateTimeKind.Local)
            {
                utc = utc.ToUniversalTime();
            }
            else if (utc.Kind == DateTimeKind.Unspecified)
            {
                utc = DateTime.SpecifyKind(utc, DateTimeKind.Utc);
            }

            return utc.ToString(CanonicalFormat, CultureInfo.InvariantCulture) + " +0000";
        }
    }
}
