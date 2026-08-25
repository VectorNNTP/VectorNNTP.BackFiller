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
    /// Resolves and canonicalizes NNTP article date values from parsed headers in deterministic candidate order.
    /// </summary>
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
        /// Tries to resolve the first canonical article date from known candidate headers.
        /// </summary>
        /// <param name="articleBytes">Original article buffer used by header slice offsets.</param>
        /// <param name="headers">Parsed header entries in original order.</param>
        /// <param name="canonicalValue">Canonical date when successful.</param>
        /// <param name="originalValue">Original selected date value bytes when successful.</param>
        /// <param name="failure">Failure reason when no candidate can be parsed.</param>
        /// <returns><see langword="true"/> when a canonical date was produced.</returns>
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
