// <copyright file="NntpMessageIdValidation.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Validation
// Temporary local NNTP Message-ID validator adapted from the Vector.NNTP.Utilities.Validation
// reference implementation, preserving INN-compatible grammar and hot-path allocation behavior.

using System.Runtime.CompilerServices;

namespace VectorNNTP.Backfiller.Runtime.Articles.Validation
{
    /// <summary>
    /// Validates NNTP Message-ID tokens using INN-compatible dot-atom grammar semantics.
    /// </summary>
    /// <remarks>
    /// This is a temporary BackFiller-local adaptation of the reference validator to avoid introducing
    /// cross-repository runtime dependencies before shared-library extraction is approved.
    /// </remarks>
    internal static class NntpMessageIdValidation
    {
        /// <summary>
        /// Maximum Message-ID length in octets accepted by current NNTP contract.
        /// </summary>
        internal const int MaxMessageIdLength = 250;

        /// <summary>
        /// Minimum Message-ID length in octets accepted by current NNTP contract.
        /// </summary>
        internal const int MinMessageIdLength = 3;

        /// <summary>
        /// Determines whether one Message-ID token is syntactically valid.
        /// </summary>
        /// <param name="messageId">Candidate Message-ID including angle brackets.</param>
        /// <param name="stripSpaces">When true, leading/trailing whitespace is trimmed before validation.</param>
        /// <returns><see langword="true"/> when valid.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsValidMessageId(ReadOnlySpan<char> messageId, bool stripSpaces = false)
        {
            int length = messageId.Length;
            if (length is 0 or > MaxMessageIdLength)
            {
                return false;
            }

            int start = 0;
            int end = length;
            if (stripSpaces)
            {
                start = NntpMessageIdValidationSimd.TrimLeadingWhitespace(messageId, start, end);
                end = NntpMessageIdValidationSimd.TrimTrailingWhitespace(messageId, start, end);
            }

            if (end - start < MinMessageIdLength)
            {
                return false;
            }

            if (!NntpMessageIdValidationSimd.IsAllAscii(messageId, start, end))
            {
                return false;
            }

            if (messageId[start] != '<')
            {
                return false;
            }

            if (!TryParseDotAtomSequence(messageId, start + 1, end, '@', out int atIndex))
            {
                return false;
            }

            int domainStart = atIndex + 1;
            int closeIndex = end - 1;
            return domainStart < closeIndex
                && messageId[closeIndex] == '>'
                && IsValidRightPartMessageId(messageId, domainStart, closeIndex, stripSpaces: false, bracket: false);
        }

        /// <summary>
        /// Determines whether one Message-ID token is syntactically valid.
        /// </summary>
        /// <param name="messageId">Candidate Message-ID string.</param>
        /// <param name="stripSpaces">When true, leading/trailing whitespace is trimmed before validation.</param>
        /// <returns><see langword="true"/> when valid.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsValidMessageId(string? messageId, bool stripSpaces = false)
        {
            return messageId is { Length: > 0 } && IsValidMessageId(messageId.AsSpan(), stripSpaces);
        }

        /// <summary>
        /// Validates right-hand Message-ID domain component.
        /// </summary>
        /// <param name="span">Source span.</param>
        /// <param name="startIndex">Inclusive domain start.</param>
        /// <param name="endIndex">Exclusive domain end.</param>
        /// <param name="stripSpaces">Whether trailing spaces should be trimmed after parse.</param>
        /// <param name="bracket">Whether closing &gt; token is required at parse end.</param>
        /// <returns><see langword="true"/> when domain syntax is valid.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsValidRightPartMessageId(
            ReadOnlySpan<char> span,
            int startIndex,
            int endIndex,
            bool stripSpaces,
            bool bracket)
        {
            if (startIndex >= endIndex)
            {
                return false;
            }

            int index;
            if (span[startIndex] == '[')
            {
                if (!TryParseDomainLiteral(span, startIndex, endIndex, out index))
                {
                    return false;
                }
            }
            else if (!TryParseDotAtomSequence(span, startIndex, endIndex, '\0', out index))
            {
                return false;
            }

            if (bracket)
            {
                if (index >= endIndex || span[index] != '>')
                {
                    return false;
                }

                index++;
            }

            if (stripSpaces)
            {
                index = NntpMessageIdValidationSimd.TrimLeadingWhitespace(span, index, endIndex);
            }

            return index == endIndex;
        }

        /// <summary>
        /// Parses dot-atom sequence until stop character or range end.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="startIndex">Inclusive start index.</param>
        /// <param name="endIndex">Exclusive end index.</param>
        /// <param name="stopChar">Stop character (or '\0' for no stop token).</param>
        /// <param name="stopIndex">Output stop index.</param>
        /// <returns><see langword="true"/> when a valid sequence was parsed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseDotAtomSequence(
            ReadOnlySpan<char> span,
            int startIndex,
            int endIndex,
            char stopChar,
            out int stopIndex)
        {
            stopIndex = startIndex;
            if (startIndex >= endIndex)
            {
                return false;
            }

            bool parsedAtom = false;
            int index = startIndex;
            while (index < endIndex)
            {
                int consumed = NntpMessageIdValidationSimd.ConsumeAtomCharacters(span, index, endIndex);
                if (consumed == 0)
                {
                    return false;
                }

                parsedAtom = true;
                index += consumed;

                if (index >= endIndex)
                {
                    stopIndex = index;
                    return parsedAtom && stopChar == '\0';
                }

                if (stopChar != '\0' && span[index] == stopChar)
                {
                    stopIndex = index;
                    return parsedAtom;
                }

                if (span[index] != '.')
                {
                    return false;
                }

                index++;
                if (index >= endIndex)
                {
                    return false;
                }
            }

            stopIndex = index;
            return parsedAtom && stopChar == '\0';
        }

        /// <summary>
        /// Parses one bracketed domain literal token.
        /// </summary>
        /// <param name="span">Input span.</param>
        /// <param name="startIndex">Opening bracket index.</param>
        /// <param name="rangeEndIndex">Exclusive range end.</param>
        /// <param name="endIndex">Output index after closing bracket.</param>
        /// <returns><see langword="true"/> when literal is valid.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseDomainLiteral(ReadOnlySpan<char> span, int startIndex, int rangeEndIndex, out int endIndex)
        {
            endIndex = startIndex;
            if (startIndex >= rangeEndIndex || span[startIndex] != '[')
            {
                return false;
            }

            int index = startIndex + 1;
            while (index < rangeEndIndex)
            {
                char current = span[index];
                if (current == ']')
                {
                    endIndex = index + 1;
                    return index > startIndex + 1;
                }

                if (!NntpMessageIdCharClasses.IsNorm(current))
                {
                    return false;
                }

                index++;
            }

            return false;
        }
    }
}
