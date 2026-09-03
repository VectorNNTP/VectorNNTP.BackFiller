// <copyright file="NntpMessageIdCharClasses.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Validation
// Temporary local Message-ID character-class bitmaps adapted from the
// Vector.NNTP.Utilities.Validation reference implementation pending future shared-library extraction.

using System.Runtime.CompilerServices;

namespace VectorNNTP.Backfiller.Runtime.Articles.Validation
{
    /// <summary>
    /// Provides allocation-free ASCII character-class tests used by the NNTP Message-ID validator.
    /// </summary>
    /// <remarks>
    /// This implementation is a temporary BackFiller-local adaptation of the reference validation subsystem.
    /// The intended future state is extraction to a dedicated shared validation package.
    /// </remarks>
    internal static class NntpMessageIdCharClasses
    {
        /// <summary>
        /// Number of 32-bit words required to represent a 256-bit bitmap.
        /// </summary>
        private const int BitmapWordCount = 8;

        /// <summary>
        /// Atom-character bitmap for local-part and dot-atom domain components.
        /// </summary>
        private static readonly uint[] AtomBitmap = CreateBitmap("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-/=?^_`{|}~");

        /// <summary>
        /// Domain-literal bitmap for characters allowed inside square-bracket literals.
        /// </summary>
        private static readonly uint[] NormBitmap = CreateBitmap("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-/=?^_`{|}~\"(),.:;<@");

        /// <summary>
        /// Determines whether a character is allowed in Message-ID atom components.
        /// </summary>
        /// <param name="value">Character to test.</param>
        /// <returns><see langword="true"/> when the character is an atom character.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAtom(char value)
        {
            uint code = value;
            return code < 128 && (AtomBitmap[code >> 5] & (1u << (int)(code & 31))) != 0;
        }

        /// <summary>
        /// Determines whether a character is allowed in Message-ID domain literals.
        /// </summary>
        /// <param name="value">Character to test.</param>
        /// <returns><see langword="true"/> when the character is allowed in domain literal text.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsNorm(char value)
        {
            uint code = value;
            return code < 128 && (NormBitmap[code >> 5] & (1u << (int)(code & 31))) != 0;
        }

        /// <summary>
        /// Builds a 256-bit lookup bitmap for the supplied ASCII character set.
        /// </summary>
        /// <param name="characters">Characters that should be marked as valid in the returned bitmap.</param>
        /// <returns>Eight 32-bit words covering the 0-255 byte range.</returns>
        private static uint[] CreateBitmap(ReadOnlySpan<char> characters)
        {
            uint[] bitmap = new uint[BitmapWordCount];
            foreach (char value in characters)
            {
                int code = value;
                bitmap[code >> 5] |= 1u << (code & 31);
            }

            return bitmap;
        }
    }
}
