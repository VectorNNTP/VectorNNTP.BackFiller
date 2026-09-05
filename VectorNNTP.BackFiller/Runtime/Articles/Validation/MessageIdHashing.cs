// <copyright file="MessageIdHashing.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Validation
// Canonical Message-ID hashing helpers used for deterministic article identity URI construction.

using System.Security.Cryptography;
using System.Text;

namespace VectorNNTP.Backfiller.Runtime.Articles.Validation
{
    /// <summary>
    /// Computes canonical hash identifiers from exact Message-ID strings.
    /// </summary>
    /// <remarks>
    /// This helper intentionally does not validate or normalize Message-ID input. Validation remains the responsibility
    /// of existing NNTP Message-ID validators and pipeline boundaries.
    /// </remarks>
    internal static class MessageIdHashing
    {
        /// <summary>
        /// Computes the canonical 32-character lowercase hexadecimal MD5 digest of the ASCII bytes for the exact supplied Message-ID string.
        /// </summary>
        /// <param name="messageId">Exact Message-ID string value from the processing pipeline.</param>
        /// <returns>Lowercase hexadecimal MD5 digest text with exactly 32 characters.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageId"/> is <see langword="null"/>.</exception>
        internal static string ComputeCanonicalMd5Hex(string messageId)
        {
            ArgumentNullException.ThrowIfNull(messageId);

            byte[] messageIdBytes = Encoding.ASCII.GetBytes(messageId);
            byte[] digest = MD5.HashData(messageIdBytes);
            return Convert.ToHexString(digest).ToLowerInvariant();
        }
    }
}
