// <copyright file="NntpArticleAcquisitionException.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Implements the nntp article acquisition exception responsibilities for this subsystem boundary.

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Typed acquisition exception carrying deterministic failure code and trace context.
    /// </summary>
    internal sealed class NntpArticleAcquisitionException : Exception
    {
        /// <summary>
        /// Initializes a new typed acquisition exception.
        /// </summary>
        /// <param name="failureCode">Deterministic acquisition classification.</param>
        /// <param name="traceContext">Operation context.</param>
        /// <param name="message">Human-readable diagnostic message.</param>
        /// <param name="innerException">Optional inner exception.</param>
        internal NntpArticleAcquisitionException(
            NntpArticleAcquisitionFailureCode failureCode,
            NntpArticleAcquisitionTraceContext traceContext,
            string message,
            Exception? innerException = null)
            : base(message, innerException)
        {
            FailureCode = failureCode;
            TraceContext = traceContext;
        }

        /// <summary>
        /// Gets deterministic failure classification.
        /// </summary>
        internal NntpArticleAcquisitionFailureCode FailureCode { get; }

        /// <summary>
        /// Gets operation trace context.
        /// </summary>
        internal NntpArticleAcquisitionTraceContext TraceContext { get; }
    }
}
