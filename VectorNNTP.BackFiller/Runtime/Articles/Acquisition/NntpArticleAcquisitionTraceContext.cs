// <copyright file="NntpArticleAcquisitionTraceContext.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: nntp article acquisition trace context in the articles acquisition subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

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
    /// Identifies the active acquisition operation for classification and logging.
    /// </summary>
    internal enum NntpArticleAcquisitionOperation
    {
        /// <summary>
        /// No specific operation is active.
        /// </summary>
        None = 0,

        /// <summary>
        /// Connection or greeting/auth initialization phase.
        /// </summary>
        Connect = 1,

        /// <summary>
        /// Command-write phase.
        /// </summary>
        CommandWrite = 2,

        /// <summary>
        /// Status-line read/parse phase.
        /// </summary>
        StatusRead = 3,

        /// <summary>
        /// Multiline article payload receive phase.
        /// </summary>
        ArticleReceive = 4,
    }

    /// <summary>
    /// Carries active operation and optional Message-ID correlation details.
    /// </summary>
    /// <param name="Operation">Current operation phase.</param>
    /// <param name="MessageId">Optional Message-ID correlation identifier.</param>
    /// <param name="MaximumValue">Optional configured maximum used by size guardrails.</param>
    /// <param name="ActualValue">Optional observed value used by size guardrails.</param>
    internal readonly record struct NntpArticleAcquisitionTraceContext(
        NntpArticleAcquisitionOperation Operation,
        string? MessageId,
        int? MaximumValue,
        int? ActualValue);
}
