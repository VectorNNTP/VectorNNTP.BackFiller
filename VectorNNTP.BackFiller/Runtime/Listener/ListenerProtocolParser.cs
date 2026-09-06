// <copyright file="ListenerProtocolParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Incremental parser for fixed-header Listener retrieval protocol frames.

using System.Buffers;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Parses Listener protocol frames incrementally from arbitrary byte sequences.
    /// </summary>
    /// <remarks>
    /// <para>This parser is transport-agnostic and does not own socket or PipeReader loops.</para>
    /// <para>It accepts fragmented or coalesced input and returns a typed status indicating incomplete, valid, or invalid frames.</para>
    /// <para>Malformed wire input is classified through <see cref="ListenerFrameParseError"/> rather than exception-driven control flow.</para>
    /// </remarks>
    internal static class ListenerProtocolParser
    {
        /// <summary>
        /// Attempts to parse exactly one frame from a contiguous byte array.
        /// </summary>
        /// <param name="input">Contiguous frame input bytes.</param>
        /// <returns>A parse result describing whether one frame was parsed, more bytes are required, or the frame is invalid.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="input"/> is <see langword="null"/>.</exception>
        internal static ListenerFrameParseResult ParseOneFrame(byte[] input)
        {
            ArgumentNullException.ThrowIfNull(input);
            ReadOnlySequence<byte> sequence = new(input);
            return ParseOneFrame(in sequence);
        }

        /// <summary>
        /// Attempts to parse exactly one frame from the provided input sequence.
        /// </summary>
        /// <param name="input">Input bytes that may contain zero, one, or many coalesced frames.</param>
        /// <returns>
        /// A parse result describing whether one frame was parsed, more bytes are required, or the frame is invalid.
        /// </returns>
        internal static ListenerFrameParseResult ParseOneFrame(in ReadOnlySequence<byte> input)
        {
            if (input.Length < ListenerProtocol.HeaderLengthBytes)
            {
                return ListenerFrameParseResult.Incomplete();
            }

            SequenceReader<byte> reader = new(input);
            Span<byte> headerBytes = stackalloc byte[ListenerProtocol.HeaderLengthBytes];
            if (!reader.TryCopyTo(headerBytes))
            {
                return ListenerFrameParseResult.Incomplete();
            }

            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(headerBytes);
            ListenerFrameParseError headerError = ValidateHeader(header);
            long frameLength = (long)ListenerProtocol.HeaderLengthBytes + header.PayloadLength;

            if (frameLength < ListenerProtocol.HeaderLengthBytes)
            {
                return ListenerFrameParseResult.Invalid(ListenerFrameParseError.InvalidFrameLength, ListenerProtocol.HeaderLengthBytes);
            }

            if (input.Length < frameLength)
            {
                return ListenerFrameParseResult.Incomplete();
            }

            if (headerError != ListenerFrameParseError.None)
            {
                return ListenerFrameParseResult.Invalid(headerError, frameLength);
            }

            reader.Advance(ListenerProtocol.HeaderLengthBytes);
            ReadOnlySequence<byte> payload = input.Slice(reader.Position, header.PayloadLength);

            ListenerFrameParseError payloadError = ValidatePayloadShape(header, payload);
            if (payloadError != ListenerFrameParseError.None)
            {
                return ListenerFrameParseResult.Invalid(payloadError, frameLength);
            }

            ListenerParsedFrame frame = new(header, payload);
            return ListenerFrameParseResult.Success(frameLength, frame);
        }

        /// <summary>
        /// Determines whether the supplied opcode is defined by the current v1 protocol contract.
        /// </summary>
        /// <param name="opcode">Raw opcode byte to evaluate.</param>
        /// <returns><see langword="true"/> when the opcode is supported in v1; otherwise <see langword="false"/>.</returns>
        internal static bool IsSupportedOpcode(byte opcode)
        {
            return opcode is (byte)ListenerOpcode.GetRequest
                or (byte)ListenerOpcode.GetResponseFound
                or (byte)ListenerOpcode.GetResponseNotFound
                or (byte)ListenerOpcode.GetResponseError
                or (byte)ListenerOpcode.GetReceiptAck;
        }

        /// <summary>
        /// Validates canonical MessageIdMd5 wire bytes.
        /// </summary>
        /// <param name="payload">Candidate payload bytes.</param>
        /// <returns><see langword="true"/> when payload is exactly 32 lowercase ASCII hex bytes; otherwise <see langword="false"/>.</returns>
        internal static bool IsValidCanonicalMessageIdMd5Payload(ReadOnlySequence<byte> payload)
        {
            if (payload.Length != ListenerProtocol.GetRequestPayloadLength)
            {
                return false;
            }

            foreach (ReadOnlyMemory<byte> segment in payload)
            {
                if (!IsValidCanonicalMessageIdMd5Payload(segment.Span))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Validates a contiguous span as canonical MessageIdMd5 bytes.
        /// </summary>
        /// <param name="payload">Contiguous candidate payload bytes.</param>
        /// <returns><see langword="true"/> when every byte is lowercase ASCII hex; otherwise <see langword="false"/>.</returns>
        internal static bool IsValidCanonicalMessageIdMd5Payload(ReadOnlySpan<byte> payload)
        {
            for (int i = 0; i < payload.Length; i++)
            {
                byte value = payload[i];
                bool digit = value is >= (byte)'0' and <= (byte)'9';
                bool lowerHex = value is >= (byte)'a' and <= (byte)'f';
                if (!digit && !lowerHex)
                {
                    return false;
                }
            }

            return true;
        }

        private static ListenerFrameParseError ValidateHeader(in ListenerFrameHeader header)
        {
            if (header.Version != ListenerProtocol.Version1)
            {
                return ListenerFrameParseError.UnsupportedVersion;
            }

            byte opcodeRaw = (byte)header.Opcode;
            return !IsSupportedOpcode(opcodeRaw)
                ? ListenerFrameParseError.UnsupportedOpcode
                : header.HeaderLength != ListenerProtocol.HeaderLengthBytes
                    ? ListenerFrameParseError.InvalidHeaderLength
                    : header.Reserved != 0
                        ? ListenerFrameParseError.InvalidReserved
                        : header.RequestId == 0
                            ? ListenerFrameParseError.InvalidRequestId
                            : ListenerFrameParseError.None;
        }

        private static ListenerFrameParseError ValidatePayloadShape(in ListenerFrameHeader header, in ReadOnlySequence<byte> payload)
        {
            switch (header.Opcode)
            {
                case ListenerOpcode.GetRequest:
                    if (header.PayloadLength != ListenerProtocol.GetRequestPayloadLength || payload.Length != ListenerProtocol.GetRequestPayloadLength)
                    {
                        return ListenerFrameParseError.InvalidFrameLength;
                    }

                    return IsValidCanonicalMessageIdMd5Payload(payload)
                        ? ListenerFrameParseError.None
                        : ListenerFrameParseError.InvalidMessageIdMd5;

                case ListenerOpcode.GetResponseNotFound:
                    if (header.PayloadLength != ListenerProtocol.GetResponseNotFoundPayloadLength || payload.Length != ListenerProtocol.GetResponseNotFoundPayloadLength)
                    {
                        return ListenerFrameParseError.InvalidFrameLength;
                    }

                    SequenceReader<byte> notFoundReader = new(payload);
                    return notFoundReader.TryRead(out byte reason) && reason == ListenerProtocol.NotFoundReasonUnavailable
                        ? ListenerFrameParseError.None
                        : ListenerFrameParseError.InvalidFrameLength;

                case ListenerOpcode.GetResponseError:
                    return header.PayloadLength == ListenerProtocol.GetResponseErrorPayloadLength && payload.Length == ListenerProtocol.GetResponseErrorPayloadLength
                        ? ListenerFrameParseError.None
                        : ListenerFrameParseError.InvalidFrameLength;

                case ListenerOpcode.GetReceiptAck:
                    return header.PayloadLength == ListenerProtocol.GetReceiptAckPayloadLength && payload.Length == ListenerProtocol.GetReceiptAckPayloadLength
                        ? ListenerFrameParseError.None
                        : ListenerFrameParseError.InvalidFrameLength;

                case ListenerOpcode.GetResponseFound:
                    return payload.Length == header.PayloadLength
                        ? ListenerFrameParseError.None
                        : ListenerFrameParseError.InvalidFrameLength;

                default:
                    return ListenerFrameParseError.UnsupportedOpcode;
            }
        }
    }
}
