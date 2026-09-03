// <copyright file="NntpArticleAcquisitionException.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Implements the nntp article acquisition exception behavior.

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Internal exception used to preserve a typed acquisition failure classification together with phase-specific trace context.
    /// </summary>
    /// <remarks>
    /// The acquisition session throws this exception internally when protocol processing needs richer state than a plain exception message.
    /// Callers typically do not observe it directly because the session maps it back into <see cref="NntpArticleAcquisitionResult"/> instances.
    /// </remarks>
    internal sealed class NntpArticleAcquisitionException : Exception
    {
        /// <summary>
        /// Initializes a new acquisition exception.
        /// </summary>
        /// <param name="failureCode">Deterministic acquisition classification to preserve.</param>
        /// <param name="traceContext">Operation phase and correlation data that were active when the failure was raised.</param>
        /// <param name="message">Human-readable diagnostic message.</param>
        /// <param name="innerException">Optional underlying exception that triggered this typed failure.</param>
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
        /// Gets the deterministic acquisition classification carried by the exception.
        /// </summary>
        /// <value>The failure code that the session should surface in its result object.</value>
        internal NntpArticleAcquisitionFailureCode FailureCode { get; }

        /// <summary>
        /// Gets the phase-specific correlation data captured when the exception was raised.
        /// </summary>
        /// <value>The active acquisition operation and any associated Message-ID or guardrail values.</value>
        internal NntpArticleAcquisitionTraceContext TraceContext { get; }
    }
}
