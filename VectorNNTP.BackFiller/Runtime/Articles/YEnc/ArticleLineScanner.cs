// <copyright file="ArticleLineScanner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: article line scanner in the articles yenc subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="ArticleLineScanner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / YEnc
// SIMD-aware and scalar fallback helpers for CRLF/LF detection and line-prefix scanning
// over raw NNTP article byte spans used by the yEnc validator.

using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace VectorNNTP.Backfiller.Runtime.Articles.YEnc
{
    /// <summary>
    /// Provides byte-level CRLF and line-prefix scanning for raw NNTP article body spans.
    /// </summary>
    internal static class ArticleLineScanner
    {
        /// <summary>
        /// Carriage-return byte used when recognizing CRLF terminators.
        /// </summary>
        private const byte CR = (byte)'\r';

        /// <summary>
        /// Line-feed byte used when recognizing CRLF and LF-only terminators.
        /// </summary>
        private const byte LF = (byte)'\n';

        /// <summary>
        /// Shuffle mask that synthesizes the previous-byte vector for the first SIMD block.
        /// </summary>
        private static readonly Vector128<byte> PrevByteShuffleIndices = Vector128.Create(
            0xFF, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

        /// <summary>
        /// Finds the first line terminator at or after <paramref name="startOffset"/> as CRLF or standalone LF.
        /// </summary>
        /// <param name="span">Article body bytes.</param>
        /// <param name="startOffset">Search start offset.</param>
        /// <returns>Index of CR or LF terminator byte, or -1 when none is found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int IndexOfCrLf(ReadOnlySpan<byte> span, int startOffset)
        {
            if ((uint)startOffset >= (uint)span.Length)
            {
                return -1;
            }

            int i = startOffset;
            int n = span.Length;
            ref byte b = ref MemoryMarshal.GetReference(span);

            if (Vector128.IsHardwareAccelerated && i + 17 <= n)
            {
                Vector128<byte> crVec = Vector128.Create(CR);
                Vector128<byte> lfVec = Vector128.Create(LF);
                Vector128<byte> allOnes = Vector128<byte>.AllBitsSet;

                while (i + 17 <= n)
                {
                    Vector128<byte> v0 = Vector128.LoadUnsafe(ref b, (nuint)i);
                    Vector128<byte> v1 = Vector128.LoadUnsafe(ref b, (nuint)(i + 1));
                    Vector128<byte> crlf = Vector128.BitwiseAnd(
                        Vector128.Equals(v0, crVec),
                        Vector128.Equals(v1, lfVec));

                    Vector128<byte> prevBytes = i > startOffset
                        ? Vector128.LoadUnsafe(ref b, (nuint)(i - 1))
                        : Vector128.Shuffle(v0, PrevByteShuffleIndices);

                    int relStart = startOffset - i;
                    Vector128<byte> atStart = Vector128<byte>.Zero;
                    if ((uint)relStart < 16)
                    {
                        atStart = Vector128.WithElement(atStart, relStart, (byte)0xFF);
                    }

                    Vector128<byte> lf = Vector128.Equals(v0, lfVec);
                    Vector128<byte> prevNotCr = Vector128.AndNot(allOnes, Vector128.Equals(prevBytes, crVec));
                    Vector128<byte> standaloneLf = Vector128.BitwiseAnd(
                        lf,
                        Vector128.BitwiseOr(atStart, prevNotCr));

                    Vector128<byte> hit = Vector128.BitwiseOr(crlf, standaloneLf);
                    uint bits = Vector128.ExtractMostSignificantBits(hit);
                    if (bits != 0)
                    {
                        return i + BitOperations.TrailingZeroCount(bits);
                    }

                    i += 16;
                }
            }

            return IndexOfCrLfScalar(ref b, n, i, startOffset);
        }

        /// <summary>
        /// Advances from a line terminator index to the first byte of the following line.
        /// </summary>
        /// <param name="span">Article body bytes.</param>
        /// <param name="lineEndIndex">Index returned by <see cref="IndexOfCrLf"/>.</param>
        /// <returns>Index immediately following the detected terminator.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int AdvancePastLineTerminator(ReadOnlySpan<byte> span, int lineEndIndex)
        {
            return (uint)lineEndIndex >= (uint)span.Length
                ? span.Length
                : span[lineEndIndex] == CR && lineEndIndex + 1 < span.Length && span[lineEndIndex + 1] == LF
                ? lineEndIndex + 2
                : lineEndIndex + 1;
        }

        /// <summary>
        /// Finds the next line beginning with <paramref name="prefix"/> at or after <paramref name="startOffset"/>.
        /// </summary>
        /// <remarks>Matches are anchored at line starts only; payload bytes in the middle of a line are not considered.</remarks>
        /// <param name="span">Article body bytes.</param>
        /// <param name="startOffset">Search start offset.</param>
        /// <param name="prefix">Byte prefix to match at line start.</param>
        /// <returns>Line start offset, or -1 when no matching line exists.</returns>
        internal static int FindLineStartingWith(ReadOnlySpan<byte> span, int startOffset, ReadOnlySpan<byte> prefix)
        {
            if ((uint)startOffset >= (uint)span.Length)
            {
                return -1;
            }

            if (prefix.IsEmpty)
            {
                return startOffset;
            }

            int lineStart = startOffset;

            while (lineStart < span.Length)
            {
                int lineEnd = IndexOfCrLf(span, lineStart);
                int lineContentEnd = lineEnd >= 0 ? lineEnd : span.Length;
                ReadOnlySpan<byte> line = span[lineStart..lineContentEnd];

                if (line.StartsWith(prefix))
                {
                    return lineStart;
                }

                if (lineEnd < 0)
                {
                    return -1;
                }

                lineStart = AdvancePastLineTerminator(span, lineEnd);
            }

            return -1;
        }

        /// <summary>
        /// Performs scalar line-terminator scanning for non-vectorized tails and fallback paths.
        /// </summary>
        /// <param name="b">Reference to the first byte in the source span.</param>
        /// <param name="n">Source span length.</param>
        /// <param name="i">Current scan index.</param>
        /// <param name="startOffset">Original search start offset.</param>
        /// <returns>Index of a detected line terminator, or -1 when none are found.</returns>
        private static int IndexOfCrLfScalar(ref byte b, int n, int i, int startOffset)
        {
            for (; i < n; i++)
            {
                if (Unsafe.Add(ref b, (nint)(uint)i) == CR && i + 1 < n && Unsafe.Add(ref b, (nint)(uint)(i + 1)) == LF)
                {
                    return i;
                }

                if (Unsafe.Add(ref b, (nint)(uint)i) == LF && (i == startOffset || Unsafe.Add(ref b, (nint)(uint)(i - 1)) != CR))
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
