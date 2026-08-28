// <copyright file="TransitDotStuffing.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    internal enum TransitDotStuffingAlgorithm
    {
        BaselineByteLoop = 0,
        BulkLineOrientedSinglePass = 1,
        BulkLineOrientedTwoPass = 2,
    }

    internal readonly record struct TransitDotStuffTransformResult(
        int BytesWritten,
        int StuffedDotCount,
        bool AppendedTrailingCrlf);

    internal static class TransitDotStuffing
    {
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
