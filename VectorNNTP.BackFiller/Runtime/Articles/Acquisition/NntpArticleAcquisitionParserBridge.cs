// <copyright file="NntpArticleAcquisitionParserBridge.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Parser integration bridge that only parses successful acquisition payloads and
// preserves acquisition failures without remapping them to parser failure codes.

using VectorNNTP.Backfiller.Runtime.Articles.Parsing;

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Bridges acquisition results into parser execution without collapsing acquisition failures into parser classifications.
    /// </summary>
    internal static class NntpArticleAcquisitionParserBridge
    {
        /// <summary>
        /// Parses the article bytes owned by a successful acquisition result.
        /// </summary>
        /// <param name="parser">Parser that will inspect the raw article bytes.</param>
        /// <param name="acquisitionResult">Acquisition result that must already own a successful article payload.</param>
        /// <returns>The parser result for <paramref name="acquisitionResult"/>'s payload.</returns>
        /// <remarks>
        /// This bridge deliberately refuses to manufacture parser failures for unsuccessful acquisitions. Callers are expected to preserve the original acquisition classification instead.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="acquisitionResult"/> did not succeed and therefore owns no article bytes.</exception>
        internal static NntpArticleParseResult ParseSuccessfulArticle(
            NntpArticleParser parser,
            NntpArticleAcquisitionResult acquisitionResult)
        {
            ArgumentNullException.ThrowIfNull(parser);
            ArgumentNullException.ThrowIfNull(acquisitionResult);

            return !acquisitionResult.IsSuccess
                ? throw new InvalidOperationException("Cannot parse article bytes from unsuccessful acquisition result.")
                : parser.Parse(acquisitionResult.ArticleBytes);
        }
    }
}
