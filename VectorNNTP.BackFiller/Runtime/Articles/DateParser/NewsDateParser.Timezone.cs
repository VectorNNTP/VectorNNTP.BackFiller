// <copyright file="NewsDateParser.Timezone.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Trailing timezone-abbreviation detection and numeric-offset substitution helpers.

using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Timezone-normalization partial for <see cref="NewsDateParser"/>.
    /// </summary>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Returns a value indicating whether a string ends in an ASCII letter.
        /// </summary>
        /// <param name="input">Non-empty input string.</param>
        /// <returns><see langword="true"/> when final character is A-Z or a-z.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool EndsWithAsciiLetter(string input)
        {
            char c = input[^1];
            return (uint)((c | 0x20) - 'a') <= 'z' - 'a';
        }

        /// <summary>
        /// Detects unknown trailing timezone abbreviations.
        /// </summary>
        /// <param name="cleaned">Trimmed normalized date string.</param>
        /// <param name="abbreviation">Unknown abbreviation when detected.</param>
        /// <returns><see langword="true"/> when an unknown abbreviation is present.</returns>
        private static bool TryGetUnknownTrailingAbbreviation(string cleaned, out string abbreviation)
        {
            abbreviation = string.Empty;
            if (cleaned.Length == 0 || !EndsWithAsciiLetter(cleaned))
            {
                return false;
            }

            Match match = CachedTimezoneAbbrRegex.Match(cleaned);
            if (!match.Success)
            {
                return false;
            }

            string abbr = match.Groups[1].Value;
            if (TimezoneMappings.ContainsKey(abbr))
            {
                return false;
            }

            abbreviation = abbr;
            return true;
        }

        /// <summary>
        /// Replaces known trailing timezone abbreviations with numeric UTC offsets.
        /// </summary>
        /// <param name="input">Normalized date string.</param>
        /// <returns>Original string when no substitution applies; otherwise substituted string.</returns>
        private static string SubstituteTimezoneAbbreviation(string input)
        {
            if (input.Length == 0 || !EndsWithAsciiLetter(input))
            {
                return input;
            }

            Match match = CachedTimezoneAbbrRegex.Match(input);
            if (!match.Success)
            {
                return input;
            }

            string abbr = match.Groups[1].Value;
            return !TimezoneMappings.TryGetValue(abbr, out string? offset)
                ? input
                : string.Concat(input.AsSpan(0, match.Index), offset.AsSpan());
        }
    }
}
