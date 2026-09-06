// <copyright file="ListenerProtocolEncoder.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Deterministic wire encoders for Listener retrieval protocol frames.

using System.Buffers.Binary;
using System.Text;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Builds Listener protocol wire headers and fixed-payload frames.
    /// </summary>
    /// <remarks>
    /// The Found response API intentionally emits header bytes separately from payload memory so callers can write
    /// retained article bytes directly without allocating an article-sized combined buffer.
    /// </remarks>
    internal static class ListenerProtocolEncoder
    {
        /// <summary>
        /// Creates an encoded GetRequest frame for the supplied canonical MessageIdMd5 string.
        /// </summary>
        /// <param name="requestId">Transport request correlation identifier. Must be non-zero.</param>
        /// <param name="messageIdMd5">Canonical lowercase 32-character hexadecimal MessageIdMd5.</param>
        /// <returns>Encoded GetRequest frame bytes.</returns>
        /// <exception cref="ArgumentException"><paramref name="messageIdMd5"/> is null/empty/whitespace or invalid format.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="requestId"/> is zero.</exception>
        internal static byte[] EncodeGetRequest(uint requestId, string messageIdMd5)
        {
            if (requestId == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(requestId), "RequestId must be greater than zero.");
            }

            if (string.IsNullOrWhiteSpace(messageIdMd5))
            {
                throw new ArgumentException("MessageIdMd5 is required.", nameof(messageIdMd5));
            }

            if (messageIdMd5.Length != ListenerProtocol.GetRequestPayloadLength)
            {
                throw new ArgumentException("MessageIdMd5 must be exactly 32 characters.", nameof(messageIdMd5));
            }

            byte[] payload = Encoding.ASCII.GetBytes(messageIdMd5);
            if (!ListenerProtocolParser.IsValidCanonicalMessageIdMd5Payload(payload.AsSpan()))
            {
                throw new ArgumentException("MessageIdMd5 must contain only lowercase hexadecimal characters [0-9a-f].", nameof(messageIdMd5));
            }

            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetRequestPayloadLength];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetRequest,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: ListenerProtocol.GetRequestPayloadLength,
                Reserved: 0);

            header.WriteTo(frame);
            payload.CopyTo(frame, ListenerProtocol.HeaderLengthBytes);
            return frame;
        }

        /// <summary>
        /// Creates a Found response representation with encoded header and caller-owned payload.
        /// </summary>
        /// <param name="requestId">Transport request correlation identifier. Must be non-zero.</param>
        /// <param name="payload">Article payload bytes that will be sent immediately after the encoded header.</param>
        /// <returns>A Found response containing 16-byte header and original payload reference.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="requestId"/> is zero.</exception>
        internal static ListenerFoundResponseFrame EncodeGetResponseFound(uint requestId, ReadOnlyMemory<byte> payload)
        {
            if (requestId == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(requestId), "RequestId must be greater than zero.");
            }

            byte[] headerBytes = new byte[ListenerProtocol.HeaderLengthBytes];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetResponseFound,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: checked((uint)payload.Length),
                Reserved: 0);

            header.WriteTo(headerBytes);
            return new ListenerFoundResponseFrame(headerBytes, payload);
        }

        /// <summary>
        /// Creates an encoded GetResponseNotFound frame.
        /// </summary>
        /// <param name="requestId">Transport request correlation identifier. Must be non-zero.</param>
        /// <returns>Encoded not-found frame bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="requestId"/> is zero.</exception>
        internal static byte[] EncodeGetResponseNotFound(uint requestId)
        {
            if (requestId == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(requestId), "RequestId must be greater than zero.");
            }

            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseNotFoundPayloadLength];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetResponseNotFound,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: ListenerProtocol.GetResponseNotFoundPayloadLength,
                Reserved: 0);

            header.WriteTo(frame);
            frame[ListenerProtocol.HeaderLengthBytes] = ListenerProtocol.NotFoundReasonUnavailable;
            return frame;
        }

        /// <summary>
        /// Creates an encoded GetResponseError frame.
        /// </summary>
        /// <param name="requestId">Transport request correlation identifier. Must be non-zero.</param>
        /// <param name="errorCode">Protocol error code written as big-endian uint16 payload.</param>
        /// <returns>Encoded error frame bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="requestId"/> is zero.</exception>
        internal static byte[] EncodeGetResponseError(uint requestId, ListenerProtocolErrorCode errorCode)
        {
            if (requestId == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(requestId), "RequestId must be greater than zero.");
            }

            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseErrorPayloadLength];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetResponseError,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: ListenerProtocol.GetResponseErrorPayloadLength,
                Reserved: 0);

            header.WriteTo(frame);
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(ListenerProtocol.HeaderLengthBytes, 2), (ushort)errorCode);
            return frame;
        }

        /// <summary>
        /// Creates an encoded GetReceiptAck frame.
        /// </summary>
        /// <param name="requestId">Transport request correlation identifier. Must be non-zero.</param>
        /// <returns>Encoded acknowledgement frame bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="requestId"/> is zero.</exception>
        internal static byte[] EncodeGetReceiptAck(uint requestId)
        {
            if (requestId == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(requestId), "RequestId must be greater than zero.");
            }

            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetReceiptAck,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: ListenerProtocol.GetReceiptAckPayloadLength,
                Reserved: 0);

            header.WriteTo(frame);
            return frame;
        }
    }
}
