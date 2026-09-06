// <copyright file="ListenerProtocol.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Wire-level protocol contracts and constants for v1 retained-article retrieval frames.

using System.Buffers;
using System.Buffers.Binary;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Defines wire-level constants for the BackFiller Listener retrieval protocol.
    /// </summary>
    /// <remarks>
    /// <para>All multibyte integer fields are encoded in network byte order (big-endian).</para>
    /// <para>Every frame starts with a fixed 16-byte header followed immediately by payload bytes.</para>
    /// <para>This contract defines retrieval-only protocol primitives and intentionally excludes historical submit operations.</para>
    /// </remarks>
    internal static class ListenerProtocol
    {
        /// <summary>
        /// Supported protocol version for current Listener wire contract.
        /// </summary>
        internal const byte Version1 = 0x01;

        /// <summary>
        /// Fixed wire header size in bytes.
        /// </summary>
        internal const ushort HeaderLengthBytes = 16;

        /// <summary>
        /// Fixed GetRequest payload size in bytes (32-character canonical MessageIdMd5 ASCII text).
        /// </summary>
        internal const uint GetRequestPayloadLength = 32;

        /// <summary>
        /// Fixed GetResponseNotFound payload size in bytes.
        /// </summary>
        internal const uint GetResponseNotFoundPayloadLength = 1;

        /// <summary>
        /// Fixed GetResponseError payload size in bytes (uint16 error code).
        /// </summary>
        internal const uint GetResponseErrorPayloadLength = 2;

        /// <summary>
        /// Fixed GetReceiptAck payload size in bytes.
        /// </summary>
        internal const uint GetReceiptAckPayloadLength = 0;

        /// <summary>
        /// Required payload byte value for the generic not-found/unavailable response reason.
        /// </summary>
        internal const byte NotFoundReasonUnavailable = 0x00;

        /// <summary>
        /// Maximum frame length representable by the protocol payload field plus fixed header.
        /// </summary>
        internal const ulong MaximumRepresentableFrameLength = (ulong)HeaderLengthBytes + uint.MaxValue;
    }

    /// <summary>
    /// Supported Listener v1 frame operation codes.
    /// </summary>
    internal enum ListenerOpcode : byte
    {
        /// <summary>
        /// Client request for retained article payload by canonical MessageIdMd5 identity.
        /// </summary>
        GetRequest = 0x01,

        /// <summary>
        /// Server response carrying retained article payload bytes.
        /// </summary>
        GetResponseFound = 0x11,

        /// <summary>
        /// Server response indicating requested article cannot currently be served.
        /// </summary>
        GetResponseNotFound = 0x12,

        /// <summary>
        /// Server response carrying a protocol-level error code.
        /// </summary>
        GetResponseError = 0x13,

        /// <summary>
        /// Client acknowledgement frame correlated to a previously served request.
        /// </summary>
        GetReceiptAck = 0x21,
    }

    /// <summary>
    /// Protocol-level error codes for GetResponseError payloads.
    /// </summary>
    internal enum ListenerProtocolErrorCode : ushort
    {
        /// <summary>
        /// Frame protocol version is not supported.
        /// </summary>
        UnsupportedVersion = 0x0001,

        /// <summary>
        /// Frame opcode is unknown or unsupported.
        /// </summary>
        UnsupportedOpcode = 0x0002,

        /// <summary>
        /// HeaderLength field is invalid.
        /// </summary>
        InvalidHeaderLength = 0x0003,

        /// <summary>
        /// Frame length or payload shape is invalid for the opcode.
        /// </summary>
        InvalidFrameLength = 0x0004,

        /// <summary>
        /// RequestId value is invalid for the operation.
        /// </summary>
        InvalidRequestId = 0x0005,

        /// <summary>
        /// RequestId duplicates an already-outstanding request.
        /// </summary>
        DuplicateRequestId = 0x0006,

        /// <summary>
        /// Connection request table is full.
        /// </summary>
        RequestTableOverflow = 0x0007,

        /// <summary>
        /// MessageIdMd5 payload format is invalid.
        /// </summary>
        InvalidMessageIdMd5 = 0x0008,

        /// <summary>
        /// Server is shutting down and cannot admit the operation.
        /// </summary>
        ServerShuttingDown = 0x0009,

        /// <summary>
        /// Internal server error occurred while processing the operation.
        /// </summary>
        InternalError = 0x000A,
    }

    /// <summary>
    /// Fixed 16-byte Listener wire header.
    /// </summary>
    /// <param name="Version">Protocol version byte.</param>
    /// <param name="Opcode">Operation code byte.</param>
    /// <param name="HeaderLength">Header length field; must be 16.</param>
    /// <param name="RequestId">Transport correlation identifier.</param>
    /// <param name="PayloadLength">Payload length in bytes.</param>
    /// <param name="Reserved">Reserved field; must be zero in v1.</param>
    /// <remarks>
    /// The header maps to byte offsets:
    /// version(0), opcode(1), headerLength(2..3), requestId(4..7), payloadLength(8..11), reserved(12..15).
    /// </remarks>
    internal readonly record struct ListenerFrameHeader(
        byte Version,
        ListenerOpcode Opcode,
        ushort HeaderLength,
        uint RequestId,
        uint PayloadLength,
        uint Reserved)
    {
        /// <summary>
        /// Writes this header into a 16-byte destination span using big-endian field encoding.
        /// </summary>
        /// <param name="destination">Destination span that must be at least 16 bytes.</param>
        /// <exception cref="ArgumentException">Thrown when <paramref name="destination"/> is shorter than 16 bytes.</exception>
        internal void WriteTo(Span<byte> destination)
        {
            if (destination.Length < ListenerProtocol.HeaderLengthBytes)
            {
                throw new ArgumentException("Destination buffer is smaller than protocol header length.", nameof(destination));
            }

            destination[0] = Version;
            destination[1] = (byte)Opcode;
            BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(2, 2), HeaderLength);
            BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(4, 4), RequestId);
            BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(8, 4), PayloadLength);
            BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(12, 4), Reserved);
        }

        /// <summary>
        /// Reads a header from a 16-byte source span using big-endian field decoding.
        /// </summary>
        /// <param name="source">Source span that must be at least 16 bytes.</param>
        /// <returns>Decoded header value.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="source"/> is shorter than 16 bytes.</exception>
        internal static ListenerFrameHeader ReadFrom(ReadOnlySpan<byte> source)
        {
            if (source.Length < ListenerProtocol.HeaderLengthBytes)
            {
                throw new ArgumentException("Source buffer is smaller than protocol header length.", nameof(source));
            }

            byte version = source[0];
            ListenerOpcode opcode = (ListenerOpcode)source[1];
            ushort headerLength = BinaryPrimitives.ReadUInt16BigEndian(source.Slice(2, 2));
            uint requestId = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(4, 4));
            uint payloadLength = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(8, 4));
            uint reserved = BinaryPrimitives.ReadUInt32BigEndian(source.Slice(12, 4));

            return new ListenerFrameHeader(version, opcode, headerLength, requestId, payloadLength, reserved);
        }
    }

    /// <summary>
    /// Parsed Listener frame envelope containing decoded header and payload bytes.
    /// </summary>
    /// <param name="Header">Decoded protocol header.</param>
    /// <param name="Payload">Payload bytes sliced from parsed input.</param>
    /// <remarks>
    /// Payload is carried as a <see cref="ReadOnlySequence{T}"/> to support fragmented/coalesced parsing without forced copies.
    /// </remarks>
    internal readonly record struct ListenerParsedFrame(
        ListenerFrameHeader Header,
        ReadOnlySequence<byte> Payload);

    /// <summary>
    /// Parser status for one incremental parse attempt.
    /// </summary>
    internal enum ListenerFrameParseStatus
    {
        /// <summary>
        /// Input did not contain a full frame yet.
        /// </summary>
        Incomplete = 0,

        /// <summary>
        /// One complete, valid frame was parsed.
        /// </summary>
        Success = 1,

        /// <summary>
        /// Input contained an invalid or unsupported frame.
        /// </summary>
        Invalid = 2,
    }

    /// <summary>
    /// Detailed parse failure classification for invalid Listener frames.
    /// </summary>
    internal enum ListenerFrameParseError
    {
        /// <summary>
        /// No parse failure (used for successful and incomplete parse outcomes).
        /// </summary>
        None = 0,

        /// <summary>
        /// Version field is unsupported.
        /// </summary>
        UnsupportedVersion = 1,

        /// <summary>
        /// Opcode field is unsupported.
        /// </summary>
        UnsupportedOpcode = 2,

        /// <summary>
        /// HeaderLength is invalid.
        /// </summary>
        InvalidHeaderLength = 3,

        /// <summary>
        /// Reserved field is non-zero.
        /// </summary>
        InvalidReserved = 4,

        /// <summary>
        /// Payload length or total frame length is invalid.
        /// </summary>
        InvalidFrameLength = 5,

        /// <summary>
        /// RequestId is invalid for the opcode.
        /// </summary>
        InvalidRequestId = 6,

        /// <summary>
        /// MessageIdMd5 payload shape or character set is invalid.
        /// </summary>
        InvalidMessageIdMd5 = 7,
    }

    /// <summary>
    /// Result of one incremental frame parse attempt.
    /// </summary>
    /// <param name="Status">Top-level parse status.</param>
    /// <param name="Error">Detailed parse error when status is invalid.</param>
    /// <param name="ConsumedBytes">Count of bytes consumed by this parse attempt.</param>
    /// <param name="Frame">Parsed frame when status is success.</param>
    /// <remarks>
    /// Invalid results consume exactly one full frame when available so callers can continue parsing subsequent coalesced frames.
    /// Incomplete results consume zero bytes and require more input.
    /// </remarks>
    internal readonly record struct ListenerFrameParseResult(
        ListenerFrameParseStatus Status,
        ListenerFrameParseError Error,
        long ConsumedBytes,
        ListenerParsedFrame? Frame)
    {
        /// <summary>
        /// Creates an incomplete parse result.
        /// </summary>
        internal static ListenerFrameParseResult Incomplete()
        {
            return new ListenerFrameParseResult(ListenerFrameParseStatus.Incomplete, ListenerFrameParseError.None, 0, null);
        }

        /// <summary>
        /// Creates a successful parse result.
        /// </summary>
        /// <param name="consumedBytes">Number of bytes consumed for the parsed frame.</param>
        /// <param name="frame">Parsed frame value.</param>
        internal static ListenerFrameParseResult Success(long consumedBytes, ListenerParsedFrame frame)
        {
            return new ListenerFrameParseResult(ListenerFrameParseStatus.Success, ListenerFrameParseError.None, consumedBytes, frame);
        }

        /// <summary>
        /// Creates an invalid parse result.
        /// </summary>
        /// <param name="error">Detailed error classification.</param>
        /// <param name="consumedBytes">Number of bytes consumed for the invalid frame.</param>
        internal static ListenerFrameParseResult Invalid(ListenerFrameParseError error, long consumedBytes)
        {
            return new ListenerFrameParseResult(ListenerFrameParseStatus.Invalid, error, consumedBytes, null);
        }
    }

    /// <summary>
    /// Encoded GetResponseFound wire frame representation that avoids article-sized payload copies.
    /// </summary>
    /// <param name="Header">Fixed 16-byte encoded frame header.</param>
    /// <param name="Payload">Caller-owned payload bytes that should be written immediately after <paramref name="Header"/>.</param>
    internal readonly record struct ListenerFoundResponseFrame(
        ReadOnlyMemory<byte> Header,
        ReadOnlyMemory<byte> Payload);
}
