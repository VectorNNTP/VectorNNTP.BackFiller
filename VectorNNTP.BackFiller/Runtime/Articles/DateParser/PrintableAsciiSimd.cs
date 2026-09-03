// <copyright file="PrintableAsciiSimd.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// SIMD-accelerated printable-ASCII validation used by the date parsing hot path.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Validates that UTF-16 code units stay within an inclusive ASCII range using vector instructions when available.
    /// </summary>
    /// <remarks>
    /// The date parser uses this helper on its hot path to reject non-printable inputs before allocating normalized strings.
    /// </remarks>
    internal static class PrintableAsciiSimd
    {
        /// <summary>
        /// Number of UTF-16 units processed in one 256-bit vector operation.
        /// </summary>
        private const int Vector256UShortCount = 16;

        /// <summary>
        /// Number of UTF-16 units processed in one 128-bit vector operation.
        /// </summary>
        private const int Vector128UShortCount = 8;

        /// <summary>
        /// Lower printable-ASCII bound (space character).
        /// </summary>
        private static readonly Vector256<ushort> PrintableLoVec256 = Vector256.Create((ushort)0x20);

        /// <summary>
        /// Inclusive printable-ASCII range width (0x7E - 0x20).
        /// </summary>
        private static readonly Vector256<ushort> PrintableRangeVec256 = Vector256.Create((ushort)0x5E);

        /// <summary>
        /// Lower printable-ASCII bound for 128-bit vectors.
        /// </summary>
        private static readonly Vector128<ushort> PrintableLoVec128 = Vector128.Create((ushort)0x20);

        /// <summary>
        /// Inclusive printable-ASCII range width for 128-bit vectors.
        /// </summary>
        private static readonly Vector128<ushort> PrintableRangeVec128 = Vector128.Create((ushort)0x5E);

        /// <summary>
        /// Determines whether every character in <paramref name="span"/> is printable ASCII.
        /// </summary>
        /// <param name="span">Input span to validate.</param>
        /// <returns><see langword="true"/> when <paramref name="span"/> is empty or every character is in the <c>0x20..0x7E</c> range.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAllPrintableAscii(ReadOnlySpan<char> span)
        {
            return span.Length == 0 || IsAllInRange(span, PrintableLoVec256, PrintableRangeVec256, PrintableLoVec128, PrintableRangeVec128, 0x20, 0x5E);
        }

        /// <summary>
        /// Determines whether all UTF-16 units in <paramref name="span"/> lie within the supplied inclusive range.
        /// </summary>
        /// <param name="span">Non-empty span to validate.</param>
        /// <param name="loVec256">256-bit vector containing the inclusive lower bound.</param>
        /// <param name="rangeVec256">256-bit vector containing the inclusive range width.</param>
        /// <param name="loVec128">128-bit vector containing the inclusive lower bound.</param>
        /// <param name="rangeVec128">128-bit vector containing the inclusive range width.</param>
        /// <param name="scalarLo">Scalar inclusive lower bound.</param>
        /// <param name="scalarRange">Scalar inclusive range width.</param>
        /// <returns><see langword="true"/> when every code unit is inside the requested range.</returns>
        /// <remarks>
        /// The helper attempts 256-bit vector scanning first, then 128-bit scanning, and finally checks the tail scalarly. The unsigned subtraction-and-compare logic rejects characters below the lower bound and above the upper bound in one comparison.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsAllInRange(
            ReadOnlySpan<char> span,
            Vector256<ushort> loVec256,
            Vector256<ushort> rangeVec256,
            Vector128<ushort> loVec128,
            Vector128<ushort> rangeVec128,
            ushort scalarLo,
            ushort scalarRange)
        {
            Debug.Assert(span.Length > 0, "IsAllInRange requires non-empty span.");

            int i = 0;
            ref ushort searchRef = ref Unsafe.As<char, ushort>(ref MemoryMarshal.GetReference(span));

            if (Vector256.IsHardwareAccelerated)
            {
                int simd256End = span.Length - Vector256UShortCount;
                while (i <= simd256End)
                {
                    Vector256<ushort> chunk = Vector256.LoadUnsafe(ref searchRef, (nuint)i);
                    Vector256<ushort> adjusted = Vector256.Subtract(chunk, loVec256);
                    Vector256<ushort> outOfRange = Vector256.GreaterThan(adjusted, rangeVec256);
                    if (!Vector256.EqualsAll(outOfRange, Vector256<ushort>.Zero))
                    {
                        return false;
                    }

                    i += Vector256UShortCount;
                }
            }

            if (Vector128.IsHardwareAccelerated)
            {
                int simd128End = span.Length - Vector128UShortCount;
                while (i <= simd128End)
                {
                    Vector128<ushort> chunk = Vector128.LoadUnsafe(ref searchRef, (nuint)i);
                    Vector128<ushort> adjusted = Vector128.Subtract(chunk, loVec128);
                    Vector128<ushort> outOfRange = Vector128.GreaterThan(adjusted, rangeVec128);
                    if (!Vector128.EqualsAll(outOfRange, Vector128<ushort>.Zero))
                    {
                        return false;
                    }

                    i += Vector128UShortCount;
                }
            }

            for (; i < span.Length; i++)
            {
                if ((uint)(span[i] - scalarLo) > scalarRange)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
