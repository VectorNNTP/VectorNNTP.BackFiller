// <copyright file="ArticleDateHeaderResolver.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Ordered candidate date-header resolver that canonicalizes the first usable article date value.

using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Resolves the first usable article date header in the parser's deterministic candidate order.
    /// </summary>
    /// <remarks>
    /// The resolver keeps scanning later candidate headers when an earlier candidate is present but malformed, allowing fallback values such as <c>Injection-Date</c> to recover otherwise acceptable articles.
    /// </remarks>
    internal static class ArticleDateHeaderResolver
    {
        /// <summary>
        /// Candidate header names checked in priority order.
        /// </summary>
        private static readonly NntpArticleHeaderName[] CandidateHeaderNames =
        [
            NntpArticleHeaderName.Date,
            NntpArticleHeaderName.InjectionDate,
            NntpArticleHeaderName.NntpPostingDate,
            NntpArticleHeaderName.Posted,
            NntpArticleHeaderName.XDate,
            NntpArticleHeaderName.DeliveryDate,
        ];

        /// <summary>
        /// Tries to resolve and canonicalize one article date from known candidate headers.
        /// </summary>
        /// <param name="articleBytes">Original article buffer that owns the header bytes referenced by <paramref name="headers"/>.</param>
        /// <param name="headers">Parsed header entries in original wire order.</param>
        /// <param name="canonicalValue">Canonical UTC date string when a candidate parses successfully.</param>
        /// <param name="originalValue">Slice of the original winning header value when resolution succeeds.</param>
        /// <param name="failure">Failure reason reported when no candidate succeeds; the implementation reports <see cref="DateParseFailureReason.ParseFailed"/> after all candidates fail.</param>
        /// <returns><see langword="true"/> when a candidate header produced a canonical value.</returns>
        internal static bool TryGetCanonicalArticleDate(
            ReadOnlyMemory<byte> articleBytes,
            IReadOnlyList<NntpArticleHeaderEntry> headers,
            out string canonicalValue,
            out ReadOnlyMemory<byte> originalValue,
            out DateParseFailureReason failure)
        {
            canonicalValue = string.Empty;
            originalValue = default;
            failure = DateParseFailureReason.Empty;

            if (headers.Count == 0)
            {
                return false;
            }

            ReadOnlySpan<byte> articleSpan = articleBytes.Span;
            for (int i = 0; i < CandidateHeaderNames.Length; i++)
            {
                NntpArticleHeaderName candidate = CandidateHeaderNames[i];
                for (int j = 0; j < headers.Count; j++)
                {
                    NntpArticleHeaderEntry entry = headers[j];
                    if (entry.KnownName != candidate)
                    {
                        continue;
                    }

                    string dateValue = Encoding.ASCII.GetString(articleSpan.Slice(entry.ValueOffset, entry.ValueLength));
                    if (NewsDateParser.TryGetCanonicalDateValue(dateValue.AsSpan(), out canonicalValue, out failure))
                    {
                        originalValue = articleBytes.Slice(entry.ValueOffset, entry.ValueLength);
                        return true;
                    }
                }
            }

            failure = DateParseFailureReason.ParseFailed;
            return false;
        }
    }
}
