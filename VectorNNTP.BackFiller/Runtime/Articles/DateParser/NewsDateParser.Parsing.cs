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
    /// Parsing-helper partial for <see cref="NewsDateParser"/>.
    /// </summary>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Attempts a fast culture-invariant parse using DateTimeOffset.TryParse.
        /// </summary>
        /// <param name="input">Trimmed input span.</param>
        /// <param name="result">Parsed UTC date when successful.</param>
        /// <returns><see langword="true"/> when parse succeeds.</returns>
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
        /// Attempts exact-format parsing using known NNTP date patterns.
        /// </summary>
        /// <param name="input">Normalized date string.</param>
        /// <param name="result">Parsed UTC date when successful.</param>
        /// <returns><see langword="true"/> when parse succeeds.</returns>
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
        /// Returns a value indicating whether the span contains adjacent ASCII spaces.
        /// </summary>
        /// <param name="s">Span to inspect.</param>
        /// <returns><see langword="true"/> when interior collapse is needed.</returns>
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
        /// Collapses runs of ASCII spaces to one space.
        /// </summary>
        /// <param name="s">Input string.</param>
        /// <returns>Original string when unchanged; otherwise collapsed string.</returns>
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
