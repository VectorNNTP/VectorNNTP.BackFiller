// <copyright file="NewsDateParser.Parsing.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Parsing helpers for quick-path and exact-format date parsing used by NewsDateParser.

using System.Globalization;
using System.Text;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Supplies the quick-parse, exact-parse, and whitespace-normalization helpers used by <see cref="NewsDateParser"/>.
    /// </summary>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Attempts the fast culture-invariant parse path before additional normalization work is allocated.
        /// </summary>
        /// <param name="input">Trimmed input span.</param>
        /// <param name="result">Parsed UTC instant when the quick parse succeeds.</param>
        /// <returns><see langword="true"/> when <see cref="DateTimeOffset.TryParse(ReadOnlySpan{char}, IFormatProvider?, DateTimeStyles, out DateTimeOffset)"/> accepted the value.</returns>
        private static bool TryQuickParse(ReadOnlySpan<char> input, out DateTime result)
        {
            if (DateTimeOffset.TryParse(input, CultureInfo.InvariantCulture, ParseStyles, out DateTimeOffset dto))
            {
                result = dto.UtcDateTime;
                return true;
            }

            result = default;
            return false;
        }

        /// <summary>
        /// Attempts exact-format parsing after the caller has applied cleanup and timezone normalization.
        /// </summary>
        /// <param name="input">Normalized date string.</param>
        /// <param name="result">Parsed UTC instant when an exact format matches.</param>
        /// <returns><see langword="true"/> when one of <see cref="DateFormats"/> matched <paramref name="input"/>.</returns>
        private static bool TryExactParse(string input, out DateTime result)
        {
            if (DateTimeOffset.TryParseExact(input, DateFormats, CultureInfo.InvariantCulture, ParseStyles, out DateTimeOffset dto))
            {
                result = dto.UtcDateTime;
                return true;
            }

            result = default;
            return false;
        }

        /// <summary>
        /// Determines whether the normalized string still contains repeated interior ASCII spaces.
        /// </summary>
        /// <param name="s">Span to inspect.</param>
        /// <returns><see langword="true"/> when a later collapse pass would remove at least one extra space.</returns>
        private static bool NeedsInteriorWhitespaceCollapse(ReadOnlySpan<char> s)
        {
            for (int i = 1; i < s.Length; i++)
            {
                if (s[i] == ' ' && s[i - 1] == ' ')
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Collapses runs of interior ASCII spaces to a single space.
        /// </summary>
        /// <param name="s">Normalized date string.</param>
        /// <returns>The original string when no collapse is needed; otherwise a new collapsed string.</returns>
        /// <remarks>
        /// Returning the original reference on the already-normalized path avoids an allocation on successful fast-follow exact parses.
        /// </remarks>
        private static string CollapseInteriorWhitespace(string s)
        {
            if (!NeedsInteriorWhitespaceCollapse(s.AsSpan()))
            {
                return s;
            }

            StringBuilder sb = new(s.Length);
            bool lastWasSpace = false;
            foreach (char c in s)
            {
                if (c == ' ')
                {
                    if (!lastWasSpace)
                    {
                        _ = sb.Append(c);
                        lastWasSpace = true;
                    }
                }
                else
                {
                    _ = sb.Append(c);
                    lastWasSpace = false;
                }
            }

            return sb.ToString();
        }
    }
}
