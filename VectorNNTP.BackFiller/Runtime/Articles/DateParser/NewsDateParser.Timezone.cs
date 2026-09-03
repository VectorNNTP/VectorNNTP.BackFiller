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
    /// Supplies helpers that detect and normalize trailing timezone abbreviations in candidate date values.
    /// </summary>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Determines whether the last character of <paramref name="input"/> is an ASCII letter.
        /// </summary>
        /// <param name="input">Non-empty normalized input string.</param>
        /// <returns><see langword="true"/> when the final character is in the <c>A-Z</c> or <c>a-z</c> range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool EndsWithAsciiLetter(string input)
        {
            char c = input[^1];
            return (uint)((c | 0x20) - 'a') <= 'z' - 'a';
        }

        /// <summary>
        /// Detects an unrecognized trailing timezone abbreviation.
        /// </summary>
        /// <param name="cleaned">Trimmed normalized date string.</param>
        /// <param name="abbreviation">The trailing abbreviation when it was present but not recognized.</param>
        /// <returns><see langword="true"/> when strict timezone validation should reject <paramref name="cleaned"/>.</returns>
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
        /// Replaces a recognized trailing timezone abbreviation with its numeric UTC offset.
        /// </summary>
        /// <param name="input">Normalized date string.</param>
        /// <returns>The original string when no recognized trailing abbreviation is present; otherwise a string with the abbreviation replaced by a numeric offset.</returns>
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
