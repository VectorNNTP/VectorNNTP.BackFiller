// <copyright file="NntpMessageIdValidationSimd.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Validation
// Temporary local SIMD/scalar helpers for Message-ID validation adapted from
// Vector.NNTP.Utilities.Validation pending future shared-library extraction.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace VectorNNTP.Backfiller.Runtime.Articles.Validation
{
    /// <summary>
    /// SIMD and scalar helper routines for hot-path Message-ID parsing.
    /// </summary>
    internal static class NntpMessageIdValidationSimd
    {
        /// <summary>
        /// Number of UTF-16 characters processed by one Vector128 lane.
        /// </summary>
        private const int Vector128CharCount = 8;

        /// <summary>
        /// Vector128 lower bound for ASCII digits.
        /// </summary>
        private static readonly Vector128<ushort> DigitLoVec128 = Vector128.Create((ushort)'0');

        /// <summary>
        /// Vector128 upper bound for ASCII digits.
        /// </summary>
        private static readonly Vector128<ushort> DigitHiVec128 = Vector128.Create((ushort)'9');

        /// <summary>
        /// Vector128 lower bound for uppercase letters.
        /// </summary>
        private static readonly Vector128<ushort> UpperLoVec128 = Vector128.Create((ushort)'A');

        /// <summary>
        /// Vector128 upper bound for uppercase letters.
        /// </summary>
        private static readonly Vector128<ushort> UpperHiVec128 = Vector128.Create((ushort)'Z');

        /// <summary>
        /// Vector128 lower bound for lowercase letters.
        /// </summary>
        private static readonly Vector128<ushort> LowerLoVec128 = Vector128.Create((ushort)'a');

        /// <summary>
        /// Vector128 upper bound for lowercase letters.
        /// </summary>
        private static readonly Vector128<ushort> LowerHiVec128 = Vector128.Create((ushort)'z');

        /// <summary>
        /// Trims ASCII whitespace from the beginning of a span range.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="start">Inclusive start index.</param>
        /// <param name="end">Exclusive end index.</param>
        /// <returns>First non-whitespace index or <paramref name="end"/>.</returns>
        /// <typeparam name="char">The char type parameter.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int TrimLeadingWhitespace(ReadOnlySpan<char> span, int start, int end)
        {
            int index = start;
            while (index < end && char.IsWhiteSpace(span[index]))
            {
                index++;
            }

            return index;
        }

        /// <summary>
        /// Trims ASCII whitespace from the end of a span range.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="start">Inclusive start index.</param>
        /// <param name="end">Exclusive end index.</param>
        /// <returns>Exclusive end index after trimming.</returns>
        /// <typeparam name="char">The char type parameter.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int TrimTrailingWhitespace(ReadOnlySpan<char> span, int start, int end)
        {
            int index = end;
            while (index > start && char.IsWhiteSpace(span[index - 1]))
            {
                index--;
            }

            return index;
        }

        /// <summary>
        /// Determines whether all characters in range are ASCII.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="start">Inclusive start index.</param>
        /// <param name="end">Exclusive end index.</param>
        /// <returns><see langword="true"/> when all characters are within 7-bit ASCII range.</returns>
        /// <typeparam name="char">The char type parameter.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAllAscii(ReadOnlySpan<char> span, int start, int end)
        {
            int index = start;
            ref ushort searchRef = ref Unsafe.As<char, ushort>(ref MemoryMarshal.GetReference(span));

            if (Vector128.IsHardwareAccelerated)
            {
                Vector128<ushort> zero = Vector128<ushort>.Zero;
                int simdEnd = end - Vector128CharCount;
                while (index <= simdEnd)
                {
                    Vector128<ushort> chunk = Vector128.LoadUnsafe(ref searchRef, (nuint)index);
                    if (!Vector128.EqualsAll(Vector128.ShiftRightLogical(chunk, 8), zero))
                    {
                        return false;
                    }

                    index += Vector128CharCount;
                }
            }

            for (; index < end; index++)
            {
                if (span[index] > 127)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Consumes a maximal prefix of atom characters from range.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="start">Inclusive start index.</param>
        /// <param name="end">Exclusive end index.</param>
        /// <returns>Consumed character count.</returns>
        /// <typeparam name="char">The char type parameter.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ConsumeAtomCharacters(ReadOnlySpan<char> span, int start, int end)
        {
            int index = start;
            while (index < end)
            {
                int alnumRun = ConsumeAlphanumericPrefix(span, index, end);
                if (alnumRun > 0)
                {
                    index += alnumRun;
                    continue;
                }

                if (!NntpMessageIdCharClasses.IsAtom(span[index]))
                {
                    break;
                }

                index++;
            }

            return index - start;
        }

        /// <summary>
        /// Consumes a maximal ASCII alphanumeric prefix from range.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="start">Inclusive start index.</param>
        /// <param name="end">Exclusive end index.</param>
        /// <returns>Consumed character count.</returns>
        /// <typeparam name="char">The char type parameter.</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ConsumeAlphanumericPrefix(ReadOnlySpan<char> span, int start, int end)
        {
            int index = start;
            if (!Vector128.IsHardwareAccelerated)
            {
                while (index < end && IsAsciiLetterOrDigit(span[index]))
                {
                    index++;
                }

                return index - start;
            }

            ref ushort searchRef = ref Unsafe.As<char, ushort>(ref MemoryMarshal.GetReference(span));
            Vector128<ushort> zero = Vector128<ushort>.Zero;
            int simdEnd = end - Vector128CharCount;

            while (index <= simdEnd)
            {
                Vector128<ushort> chunk = Vector128.LoadUnsafe(ref searchRef, (nuint)index);
                if (!Vector128.EqualsAll(Vector128.ShiftRightLogical(chunk, 8), zero))
                {
                    break;
                }

                Vector128<ushort> isDigit = Vector128.BitwiseAnd(
                    Vector128.GreaterThanOrEqual(chunk, DigitLoVec128),
                    Vector128.LessThanOrEqual(chunk, DigitHiVec128));

                Vector128<ushort> isUpper = Vector128.BitwiseAnd(
                    Vector128.GreaterThanOrEqual(chunk, UpperLoVec128),
                    Vector128.LessThanOrEqual(chunk, UpperHiVec128));

                Vector128<ushort> isLower = Vector128.BitwiseAnd(
                    Vector128.GreaterThanOrEqual(chunk, LowerLoVec128),
                    Vector128.LessThanOrEqual(chunk, LowerHiVec128));

                Vector128<ushort> valid = Vector128.BitwiseOr(isDigit, Vector128.BitwiseOr(isUpper, isLower));
                if (!Vector128.EqualsAll(valid, Vector128<ushort>.AllBitsSet))
                {
                    break;
                }

                index += Vector128CharCount;
            }

            while (index < end && IsAsciiLetterOrDigit(span[index]))
            {
                index++;
            }

            return index - start;
        }

        /// <summary>
        /// Determines whether one character is ASCII letter or digit.
        /// </summary>
        /// <param name="value">Character to test.</param>
        /// <returns><see langword="true"/> when ASCII alphanumeric.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsAsciiLetterOrDigit(char value)
        {
            return (uint)(value - '0') <= 9
                || (uint)(value - 'A') <= 25
                || (uint)(value - 'a') <= 25;
        }
    }
}
