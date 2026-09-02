// <copyright file="DateParseFailureReason.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: date parse failure reason in the articles date parser subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="DateParseFailureReason.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Failure-classification values for date parsing and canonicalization of NNTP article date headers.

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Describes why a date header value could not be parsed or canonicalized.
    /// </summary>
    internal enum DateParseFailureReason
    {
        /// <summary>
        /// Parsing succeeded.
        /// </summary>
        None = 0,

        /// <summary>
        /// Input was empty or whitespace.
        /// </summary>
        Empty = 1,

        /// <summary>
        /// Input exceeded configured maximum length.
        /// </summary>
        TooLong = 2,

        /// <summary>
        /// Input contained non-printable ASCII characters.
        /// </summary>
        NonPrintableAscii = 3,

        /// <summary>
        /// Reserved for fixed-buffer normalization paths.
        /// </summary>
        NormalizationBufferTooSmall = 4,

        /// <summary>
        /// Input ended with an unknown timezone abbreviation while strict abbreviation mode was enabled.
        /// </summary>
        UnknownTimezoneAbbreviation = 5,

        /// <summary>
        /// No parsing strategy accepted the input value.
        /// </summary>
        ParseFailed = 6,
    }
}
