// <copyright file="NntpArticleHeaderValueUnfolder.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Parsing
// Bounded semantic header-value unfolding that preserves raw wire slices for parser contracts.

namespace VectorNNTP.Backfiller.Runtime.Articles.Parsing
{
    /// <summary>
    /// Provides bounded unfolding of folded header values for semantic validation without modifying retained raw article slices.
    /// </summary>
    internal static class NntpArticleHeaderValueUnfolder
    {
        /// <summary>
        /// Attempts to unfold one raw header-value slice into a semantic byte view suitable for validators.
        /// </summary>
        /// <param name="rawValue">Raw header-value bytes, including any continuation line terminators and leading continuation whitespace.</param>
        /// <param name="destination">Bounded destination buffer receiving unfolded semantic bytes.</param>
        /// <param name="bytesWritten">Number of semantic bytes written when unfolding succeeds.</param>
        /// <returns><see langword="true"/> when unfolding succeeds within bounds and all embedded line breaks represent valid continuation boundaries.</returns>
        /// <remarks>
        /// This method preserves ordinary bytes exactly and normalizes each valid fold boundary (CRLF/CR/LF followed by SP/HTAB) to one ASCII space.
        /// Raw parser slices remain unchanged; only the semantic view is unfolded.
        /// </remarks>
        internal static bool TryUnfold(ReadOnlySpan<byte> rawValue, Span<byte> destination, out int bytesWritten)
        {
            bytesWritten = 0;

            for (int i = 0; i < rawValue.Length; i++)
            {
                byte b = rawValue[i];
                if (b == (byte)'\r' || b == (byte)'\n')
                {
                    int continuationStart = i + 1;
                    if (b == (byte)'\r' && continuationStart < rawValue.Length && rawValue[continuationStart] == (byte)'\n')
                    {
                        continuationStart++;
                    }

                    if (continuationStart >= rawValue.Length)
                    {
                        return false;
                    }

                    byte continuationPrefix = rawValue[continuationStart];
                    if (continuationPrefix is not (byte)' ' and not (byte)'\t')
                    {
                        return false;
                    }

                    if (bytesWritten >= destination.Length)
                    {
                        return false;
                    }

                    destination[bytesWritten++] = (byte)' ';
                    i = continuationStart;
                    continue;
                }

                if (bytesWritten >= destination.Length)
                {
                    return false;
                }

                destination[bytesWritten++] = b;
            }

            return true;
        }
    }
}
