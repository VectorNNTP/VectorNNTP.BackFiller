// <copyright file="NntpArticleAcquisitionParserBridge.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: nntp article acquisition parser bridge in the articles acquisition subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

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
    /// Bridges successful acquisition outputs to parser execution.
    /// </summary>
    internal static class NntpArticleAcquisitionParserBridge
    {
        /// <summary>
        /// Parses article bytes from a successful acquisition result.
        /// </summary>
        /// <param name="parser">Parser instance.</param>
        /// <param name="acquisitionResult">Acquisition result.</param>
        /// <returns>Parse result for successful acquisition payload.</returns>
        /// <exception cref="InvalidOperationException">Thrown when acquisition did not succeed.</exception>
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
