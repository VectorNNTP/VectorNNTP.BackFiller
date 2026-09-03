// <copyright file="TransitDotStuffing.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit dot stuffing behavior.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Selects the dot-stuffing implementation used to escape article lines that begin with a dot.
    /// </summary>
    internal enum TransitDotStuffingAlgorithm
    {
        /// <summary>
        /// Processes the payload one byte at a time while tracking line starts.
        /// </summary>
        BaselineByteLoop = 0,

        /// <summary>
        /// Processes the payload one line at a time in a single pass.
        /// </summary>
        BulkLineOrientedSinglePass = 1,

        /// <summary>
        /// Computes the required output length first, then performs a second pass to write the transformed payload.
        /// </summary>
        BulkLineOrientedTwoPass = 2,
    }

    /// <summary>
    /// Describes the output of a dot-stuffing transform.
    /// </summary>
    /// <param name="BytesWritten">Number of bytes written to the destination span.</param>
    /// <param name="StuffedDotCount">Number of leading dots duplicated to preserve NNTP transparency rules.</param>
    /// <param name="AppendedTrailingCrlf">Indicates whether the transform appended a trailing CRLF because the source did not end in LF.</param>
    internal readonly record struct TransitDotStuffTransformResult(
        int BytesWritten,
        int StuffedDotCount,
        bool AppendedTrailingCrlf);

    /// <summary>
    /// Provides allocation-free NNTP dot-stuffing helpers for staging article payloads into outbound buffers.
    /// </summary>
    internal static class TransitDotStuffing
    {
        /// <summary>
        /// Calculates the destination size required after dot-stuffing and optional trailing-CRLF insertion.
        /// </summary>
        /// <param name="source">Source article bytes.</param>
        /// <param name="appendTrailingCrlfWhenMissingLf"><see langword="true"/> to append CRLF when the source does not end in LF.</param>
        /// <param name="stuffedDotCount">Receives the number of line-leading dots that must be duplicated.</param>
        /// <returns>The number of bytes required in the destination span.</returns>
        internal static int GetRequiredDestinationLength(ReadOnlySpan<byte> source, bool appendTrailingCrlfWhenMissingLf, out int stuffedDotCount)
        {
            stuffedDotCount = 0;

            if (!source.IsEmpty && source[0] == (byte)'.')
            {
                stuffedDotCount++;
            }

            int searchOffset = 0;
            while (searchOffset < source.Length)
            {
                int newlineOffset = source[searchOffset..].IndexOf((byte)'\n');
                if (newlineOffset < 0)
                {
                    break;
                }

                int nextIndex = searchOffset + newlineOffset + 1;
                if (nextIndex < source.Length && source[nextIndex] == (byte)'.')
                {
                    stuffedDotCount++;
                }

                searchOffset = nextIndex;
            }

            int required = source.Length + stuffedDotCount;

            if (appendTrailingCrlfWhenMissingLf && !source.IsEmpty && source[^1] != (byte)'\n')
            {
                required += 2;
            }

            return required;
        }

        /// <summary>
        /// Transforms the source payload into a dot-stuffed destination using the selected algorithm.
        /// </summary>
        /// <param name="source">Original article payload.</param>
        /// <param name="destination">Destination span that receives the transformed payload.</param>
        /// <param name="result">Receives transform metrics when the operation succeeds.</param>
        /// <param name="algorithm">Dot-stuffing strategy to execute.</param>
        /// <param name="appendTrailingCrlfWhenMissingLf"><see langword="true"/> to append CRLF when the source does not end in LF.</param>
        /// <returns><see langword="true"/> when the destination span was large enough; otherwise <see langword="false"/>.</returns>
        internal static bool TryDotStuff(
            ReadOnlySpan<byte> source,
            Span<byte> destination,
            out TransitDotStuffTransformResult result,
            TransitDotStuffingAlgorithm algorithm = TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass,
            bool appendTrailingCrlfWhenMissingLf = true)
        {
            return algorithm switch
            {
                TransitDotStuffingAlgorithm.BaselineByteLoop => TryDotStuffBaselineByteLoop(source, destination, out result, appendTrailingCrlfWhenMissingLf),
                TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass => TryDotStuffBulkLineOrientedSinglePass(source, destination, out result, appendTrailingCrlfWhenMissingLf),
                TransitDotStuffingAlgorithm.BulkLineOrientedTwoPass => TryDotStuffBulkLineOrientedTwoPass(source, destination, out result, appendTrailingCrlfWhenMissingLf),
                _ => TryDotStuffBulkLineOrientedSinglePass(source, destination, out result, appendTrailingCrlfWhenMissingLf),
            };
        }

        /// <summary>
        /// Dot-stuffs the payload with a byte-at-a-time baseline implementation.
        /// </summary>
        /// <param name="source">Original article payload.</param>
        /// <param name="destination">Destination span that receives the transformed payload.</param>
        /// <param name="result">Receives transform metrics when the operation succeeds.</param>
        /// <param name="appendTrailingCrlfWhenMissingLf"><see langword="true"/> to append CRLF when the source does not end in LF.</param>
        /// <returns><see langword="true"/> when the destination span was large enough; otherwise <see langword="false"/>.</returns>
        internal static bool TryDotStuffBaselineByteLoop(
            ReadOnlySpan<byte> source,
            Span<byte> destination,
            out TransitDotStuffTransformResult result,
            bool appendTrailingCrlfWhenMissingLf = true)
        {
            int writeIndex = 0;
            int stuffedDotCount = 0;
            bool atLineStart = true;

            for (int i = 0; i < source.Length; i++)
            {
                byte current = source[i];

                if (atLineStart && current == (byte)'.')
                {
                    if (writeIndex >= destination.Length)
                    {
                        result = default;
                        return false;
                    }

                    destination[writeIndex++] = (byte)'.';
                    stuffedDotCount++;
                }

                if (writeIndex >= destination.Length)
                {
                    result = default;
                    return false;
                }

                destination[writeIndex++] = current;
                atLineStart = current == (byte)'\n';
            }

            bool appendedTrailingCrlf = false;
            if (appendTrailingCrlfWhenMissingLf && !source.IsEmpty && source[^1] != (byte)'\n')
            {
                if (writeIndex + 2 > destination.Length)
                {
                    result = default;
                    return false;
                }

                destination[writeIndex++] = (byte)'\r';
                destination[writeIndex++] = (byte)'\n';
                appendedTrailingCrlf = true;
            }

            result = new TransitDotStuffTransformResult(writeIndex, stuffedDotCount, appendedTrailingCrlf);
            return true;
        }

        /// <summary>
        /// Dot-stuffs the payload by copying one logical line at a time in a single pass.
        /// </summary>
        /// <param name="source">Original article payload.</param>
        /// <param name="destination">Destination span that receives the transformed payload.</param>
        /// <param name="result">Receives transform metrics when the operation succeeds.</param>
        /// <param name="appendTrailingCrlfWhenMissingLf"><see langword="true"/> to append CRLF when the source does not end in LF.</param>
        /// <returns><see langword="true"/> when the destination span was large enough; otherwise <see langword="false"/>.</returns>
        internal static bool TryDotStuffBulkLineOrientedSinglePass(
            ReadOnlySpan<byte> source,
            Span<byte> destination,
            out TransitDotStuffTransformResult result,
            bool appendTrailingCrlfWhenMissingLf = true)
        {
            int writeIndex = 0;
            int stuffedDotCount = 0;
            int lineStart = 0;

            while (lineStart < source.Length)
            {
                if (source[lineStart] == (byte)'.')
                {
                    if (writeIndex >= destination.Length)
                    {
                        result = default;
                        return false;
                    }

                    destination[writeIndex++] = (byte)'.';
                    stuffedDotCount++;
                }

                int newlineOffset = source[lineStart..].IndexOf((byte)'\n');
                int lineLength = newlineOffset < 0 ? source.Length - lineStart : newlineOffset + 1;
                if (writeIndex + lineLength > destination.Length)
                {
                    result = default;
                    return false;
                }

                source.Slice(lineStart, lineLength).CopyTo(destination[writeIndex..]);
                writeIndex += lineLength;
                lineStart += lineLength;
            }

            bool appendedTrailingCrlf = false;
            if (appendTrailingCrlfWhenMissingLf && !source.IsEmpty && source[^1] != (byte)'\n')
            {
                if (writeIndex + 2 > destination.Length)
                {
                    result = default;
                    return false;
                }

                destination[writeIndex++] = (byte)'\r';
                destination[writeIndex++] = (byte)'\n';
                appendedTrailingCrlf = true;
            }

            result = new TransitDotStuffTransformResult(writeIndex, stuffedDotCount, appendedTrailingCrlf);
            return true;
        }

        /// <summary>
        /// Dot-stuffs the payload with a two-pass implementation that validates capacity before writing.
        /// </summary>
        /// <param name="source">Original article payload.</param>
        /// <param name="destination">Destination span that receives the transformed payload.</param>
        /// <param name="result">Receives transform metrics when the operation succeeds.</param>
        /// <param name="appendTrailingCrlfWhenMissingLf"><see langword="true"/> to append CRLF when the source does not end in LF.</param>
        /// <returns><see langword="true"/> when the destination span was large enough; otherwise <see langword="false"/>.</returns>
        internal static bool TryDotStuffBulkLineOrientedTwoPass(
            ReadOnlySpan<byte> source,
            Span<byte> destination,
            out TransitDotStuffTransformResult result,
            bool appendTrailingCrlfWhenMissingLf = true)
        {
            int requiredLength = GetRequiredDestinationLength(source, appendTrailingCrlfWhenMissingLf, out int stuffedDotCount);
            if (requiredLength > destination.Length)
            {
                result = default;
                return false;
            }

            int writeIndex = 0;
            int lineStart = 0;

            while (lineStart < source.Length)
            {
                if (source[lineStart] == (byte)'.')
                {
                    destination[writeIndex++] = (byte)'.';
                }

                int newlineOffset = source[lineStart..].IndexOf((byte)'\n');
                int lineLength = newlineOffset < 0 ? source.Length - lineStart : newlineOffset + 1;
                source.Slice(lineStart, lineLength).CopyTo(destination[writeIndex..]);
                writeIndex += lineLength;
                lineStart += lineLength;
            }

            bool appendedTrailingCrlf = false;
            if (appendTrailingCrlfWhenMissingLf && !source.IsEmpty && source[^1] != (byte)'\n')
            {
                destination[writeIndex++] = (byte)'\r';
                destination[writeIndex++] = (byte)'\n';
                appendedTrailingCrlf = true;
            }

            result = new TransitDotStuffTransformResult(writeIndex, stuffedDotCount, appendedTrailingCrlf);
            return true;
        }
    }
}
