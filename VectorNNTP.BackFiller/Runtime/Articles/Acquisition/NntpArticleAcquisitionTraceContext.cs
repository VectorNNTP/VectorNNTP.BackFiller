// <copyright file="NntpArticleAcquisitionTraceContext.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Lightweight per-request correlation context for deterministic exception classification
// and structured logging across ARTICLE command, response, and payload phases.

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Identifies the acquisition phase that was active when session code classified or raised a failure.
    /// </summary>
    internal enum NntpArticleAcquisitionOperation
    {
        /// <summary>
        /// No phase has been associated with the current context.
        /// </summary>
        None = 0,

        /// <summary>
        /// Session establishment, greeting validation, or authentication setup is in progress.
        /// </summary>
        Connect = 1,

        /// <summary>
        /// An NNTP command line is being transmitted.
        /// </summary>
        CommandWrite = 2,

        /// <summary>
        /// A single-line NNTP status response is being read and parsed.
        /// </summary>
        StatusRead = 3,

        /// <summary>
        /// A multiline article payload is being received.
        /// </summary>
        ArticleReceive = 4,
    }

    /// <summary>
    /// Carries lightweight correlation data for one acquisition operation.
    /// </summary>
    /// <param name="Operation">Acquisition phase that owns the current work.</param>
    /// <param name="MessageId">ARTICLE Message-ID correlation value when the operation is article-specific.</param>
    /// <param name="MaximumValue">Configured maximum involved in a guardrail failure, such as line or article size limits.</param>
    /// <param name="ActualValue">Observed value that exceeded or was compared against <paramref name="MaximumValue"/>.</param>
    internal readonly record struct NntpArticleAcquisitionTraceContext(
        NntpArticleAcquisitionOperation Operation,
        string? MessageId,
        int? MaximumValue,
        int? ActualValue);
}
