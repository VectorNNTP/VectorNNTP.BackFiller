// <copyright file="ArticleResourceLimits.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles
// Canonical hard protocol/resource limits shared by acquisition and parser paths.

namespace VectorNNTP.Backfiller.Runtime.Articles
{
    /// <summary>
    /// Defines the repository-wide hard resource boundaries for NNTP article processing.
    /// </summary>
    /// <remarks>
    /// <para>These limits are protocol safety boundaries, not runtime configuration settings.</para>
    /// <para>The line boundary is specified as 1024 characters by issue contract and is enforced on wire bytes because parser framing is ASCII byte-oriented NNTP line processing.</para>
    /// </remarks>
    internal static class ArticleResourceLimits
    {
        /// <summary>
        /// Maximum accepted ARTICLE payload size in bytes.
        /// </summary>
        internal const int MaxArticleBytes = 5 * 1024 * 1024;

        /// <summary>
        /// Maximum accepted ARTICLE/header line length in characters.
        /// </summary>
        internal const int MaxArticleLineCharacters = 1024;

        /// <summary>
        /// Maximum accepted ARTICLE/header line length in wire bytes for ASCII NNTP framing.
        /// </summary>
        internal const int MaxArticleLineBytes = MaxArticleLineCharacters;
    }
}
