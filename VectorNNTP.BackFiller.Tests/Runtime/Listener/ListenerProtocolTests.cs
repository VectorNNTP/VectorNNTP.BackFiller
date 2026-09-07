// <copyright file="ListenerProtocolTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Listener
// Focused unit tests for Listener wire-level protocol contracts, parser, and encoders.

using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Listener;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Listener
{
    /// <summary>
    /// Verifies Listener wire-level protocol framing, parsing, validation, and deterministic encoding behavior.
    /// </summary>
    public sealed class ListenerProtocolTests
    {
        [Fact]
        public void ListenerFrameHeader_WriteRead_RoundTripsBigEndianValues()
        {
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetRequest,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: 0x01020304,
                PayloadLength: 0xA1B2C3D4,
                Reserved: 0);

            byte[] bytes = new byte[ListenerProtocol.HeaderLengthBytes];
            header.WriteTo(bytes);

            Assert.Equal(ListenerProtocol.Version1, bytes[0]);
            Assert.Equal((byte)ListenerOpcode.GetRequest, bytes[1]);
            Assert.Equal((ushort)0x0010, BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(2, 2)));
            Assert.Equal(0x01020304U, BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(4, 4)));
            Assert.Equal(0xA1B2C3D4U, BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(8, 4)));
            Assert.Equal(0U, BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(12, 4)));

            ListenerFrameHeader roundTripped = ListenerFrameHeader.ReadFrom(bytes);
            Assert.Equal(header, roundTripped);
        }

        [Fact]
        public void ParseOneFrame_WhenUnsupportedVersion_ReturnsUnsupportedVersionError()
        {
            byte[] frame = CreateFrame(
                version: 0x02,
                opcode: ListenerOpcode.GetReceiptAck,
                requestId: 1,
                payload: []);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.UnsupportedVersion, result.Error);
            Assert.Equal(frame.Length, result.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenUnknownOpcode_ReturnsUnsupportedOpcodeError()
        {
            byte[] frame = CreateRawFrame(
                version: ListenerProtocol.Version1,
                opcode: 0x99,
                headerLength: ListenerProtocol.HeaderLengthBytes,
                requestId: 1,
                payloadLength: 0,
                reserved: 0,
                payload: []);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.UnsupportedOpcode, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenHeaderLengthInvalid_ReturnsInvalidHeaderLengthError()
        {
            byte[] frame = CreateRawFrame(
                version: ListenerProtocol.Version1,
                opcode: (byte)ListenerOpcode.GetReceiptAck,
                headerLength: 15,
                requestId: 1,
                payloadLength: 0,
                reserved: 0,
                payload: []);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidHeaderLength, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenReservedNonZero_ReturnsInvalidReservedError()
        {
            byte[] frame = CreateRawFrame(
                version: ListenerProtocol.Version1,
                opcode: (byte)ListenerOpcode.GetReceiptAck,
                headerLength: ListenerProtocol.HeaderLengthBytes,
                requestId: 1,
                payloadLength: 0,
                reserved: 1,
                payload: []);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidReserved, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenRequestIdZero_ReturnsInvalidRequestIdError()
        {
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetReceiptAck,
                requestId: 0,
                payload: []);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidRequestId, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenGetRequestPayloadLengthNot32_ReturnsInvalidFrameLength()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe16");
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 5,
                payload: payload);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidFrameLength, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenGetRequestPayloadIsLowercaseHex_Accepts()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe160");
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 9,
                payload: payload);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Success, result.Status);
            Assert.NotNull(result.Frame);
            Assert.Equal(ListenerOpcode.GetRequest, result.Frame.Value.Header.Opcode);
            Assert.Equal(9U, result.Frame.Value.Header.RequestId);
            Assert.Equal(ListenerProtocol.GetRequestPayloadLength, result.Frame.Value.Header.PayloadLength);
            Assert.Equal(frame.Length, result.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenGetRequestPayloadContainsUppercaseHex_ReturnsInvalidMessageIdMd5()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30EDC94157AA16FE644A45A1F1FFE160");
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 2,
                payload: payload);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidMessageIdMd5, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenGetRequestPayloadContainsNonHex_ReturnsInvalidMessageIdMd5()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe16g");
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 3,
                payload: payload);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidMessageIdMd5, result.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenGetRequestPayloadContainsNonAscii_ReturnsInvalidMessageIdMd5()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe16?");
            payload[^1] = 0xFF;
            byte[] frame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 4,
                payload: payload);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(frame);

            Assert.Equal(ListenerFrameParseStatus.Invalid, result.Status);
            Assert.Equal(ListenerFrameParseError.InvalidMessageIdMd5, result.Error);
        }

        [Fact]
        public void EncodeGetResponseFound_ReturnsHeaderAndOriginalPayloadWithoutCopyRequirement()
        {
            byte[] payload = Enumerable.Range(0, 12).Select(static x => (byte)x).ToArray();

            ListenerFoundResponseFrame found = ListenerProtocolEncoder.EncodeGetResponseFound(7, payload);

            Assert.Equal(payload, found.Payload.ToArray());
            Assert.Equal(payload.Length, found.Payload.Length);
            Assert.Equal(ListenerProtocol.HeaderLengthBytes, found.Header.Length);
            Assert.False(found.Payload.IsEmpty);

            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(found.Header.Span);
            Assert.Equal(ListenerProtocol.Version1, header.Version);
            Assert.Equal(ListenerOpcode.GetResponseFound, header.Opcode);
            Assert.Equal(7U, header.RequestId);
            Assert.Equal((uint)payload.Length, header.PayloadLength);
            Assert.Equal(0U, header.Reserved);
        }

        [Fact]
        public void EncodeGetResponseNotFound_EncodesExpectedHeaderAndReasonByte()
        {
            byte[] frame = ListenerProtocolEncoder.EncodeGetResponseNotFound(10);

            Assert.Equal(ListenerProtocol.HeaderLengthBytes + 1, frame.Length);
            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(frame);
            Assert.Equal(ListenerOpcode.GetResponseNotFound, header.Opcode);
            Assert.Equal(10U, header.RequestId);
            Assert.Equal(1U, header.PayloadLength);
            Assert.Equal(0U, header.Reserved);
            Assert.Equal(ListenerProtocol.NotFoundReasonUnavailable, frame[ListenerProtocol.HeaderLengthBytes]);
        }

        [Fact]
        public void EncodeGetResponseError_EncodesExpectedHeaderAndBigEndianErrorCode()
        {
            byte[] frame = ListenerProtocolEncoder.EncodeGetResponseError(11, ListenerProtocolErrorCode.InternalError);

            Assert.Equal(ListenerProtocol.HeaderLengthBytes + 2, frame.Length);
            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(frame);
            Assert.Equal(ListenerOpcode.GetResponseError, header.Opcode);
            Assert.Equal(11U, header.RequestId);
            Assert.Equal(2U, header.PayloadLength);
            Assert.Equal((ushort)ListenerProtocolErrorCode.InternalError, BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(ListenerProtocol.HeaderLengthBytes, 2)));
        }

        [Fact]
        public void EncodeGetReceiptAck_EncodesZeroPayloadHeader()
        {
            byte[] frame = ListenerProtocolEncoder.EncodeGetReceiptAck(12);

            Assert.Equal(ListenerProtocol.HeaderLengthBytes, frame.Length);
            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(frame);
            Assert.Equal(ListenerOpcode.GetReceiptAck, header.Opcode);
            Assert.Equal(12U, header.RequestId);
            Assert.Equal(0U, header.PayloadLength);
            Assert.Equal(0U, header.Reserved);
        }

        [Fact]
        public void ParseOneFrame_WhenFixedOpcodePayloadLengthInvalid_ReturnsInvalidFrameLength()
        {
            byte[] invalidAck = CreateRawFrame(
                version: ListenerProtocol.Version1,
                opcode: (byte)ListenerOpcode.GetReceiptAck,
                headerLength: ListenerProtocol.HeaderLengthBytes,
                requestId: 1,
                payloadLength: 1,
                reserved: 0,
                payload: [0x00]);

            ListenerFrameParseResult ackResult = Parse(invalidAck);

            Assert.Equal(ListenerFrameParseStatus.Invalid, ackResult.Status);
            Assert.Equal(ListenerFrameParseError.InvalidFrameLength, ackResult.Error);
        }

        [Fact]
        public void ParseOneFrame_WhenInputContainsOnlyPartialHeader_ReturnsIncomplete()
        {
            byte[] bytes = new byte[10];

            ListenerFrameParseResult result = Parse(bytes);

            Assert.Equal(ListenerFrameParseStatus.Incomplete, result.Status);
            Assert.Equal(0, result.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenFrameHeaderArrivesBeforePayload_ReturnsIncompleteThenSuccess()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe160");
            byte[] fullFrame = CreateFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 19,
                payload: payload);

            ReadOnlySequence<byte> headerOnly = new(fullFrame.AsMemory(0, ListenerProtocol.HeaderLengthBytes));
            ListenerFrameParseResult first = ListenerProtocolParser.ParseOneFrame(in headerOnly);

            Assert.Equal(ListenerFrameParseStatus.Incomplete, first.Status);

            ListenerFrameParseResult second = Parse(fullFrame);
            Assert.Equal(ListenerFrameParseStatus.Success, second.Status);
            Assert.Equal(fullFrame.Length, second.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenInputContainsCoalescedFrames_ParsesSequentially()
        {
            byte[] frameA = ListenerProtocolEncoder.EncodeGetReceiptAck(21);
            byte[] frameB = ListenerProtocolEncoder.EncodeGetResponseError(22, ListenerProtocolErrorCode.UnsupportedOpcode);
            byte[] combined = new byte[frameA.Length + frameB.Length];
            Buffer.BlockCopy(frameA, 0, combined, 0, frameA.Length);
            Buffer.BlockCopy(frameB, 0, combined, frameA.Length, frameB.Length);

            ReadOnlySequence<byte> sequence = new(combined);
            ListenerFrameParseResult first = ListenerProtocolParser.ParseOneFrame(in sequence);

            Assert.Equal(ListenerFrameParseStatus.Success, first.Status);
            Assert.Equal(frameA.Length, first.ConsumedBytes);

            ReadOnlySequence<byte> remaining = sequence.Slice(first.ConsumedBytes);
            ListenerFrameParseResult second = ListenerProtocolParser.ParseOneFrame(in remaining);

            Assert.Equal(ListenerFrameParseStatus.Success, second.Status);
            Assert.Equal(frameB.Length, second.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenInputEmpty_ReturnsIncomplete()
        {
            ReadOnlySequence<byte> empty = ReadOnlySequence<byte>.Empty;
            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(in empty);

            Assert.Equal(ListenerFrameParseStatus.Incomplete, result.Status);
            Assert.Equal(0, result.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenTruncatedFrame_ReturnsIncomplete()
        {
            byte[] full = ListenerProtocolEncoder.EncodeGetResponseError(30, ListenerProtocolErrorCode.InternalError);
            byte[] truncated = full.Take(full.Length - 1).ToArray();

            ListenerFrameParseResult result = Parse(truncated);

            Assert.Equal(ListenerFrameParseStatus.Incomplete, result.Status);
            Assert.Equal(0, result.ConsumedBytes);
        }

        [Fact]
        public void ParseOneFrame_WhenPayloadLengthMaxUInt32AndFrameIncomplete_ReturnsIncompleteWithoutAllocation()
        {
            byte[] headerOnly = new byte[ListenerProtocol.HeaderLengthBytes];
            ListenerFrameHeader header = new(
                Version: ListenerProtocol.Version1,
                Opcode: ListenerOpcode.GetResponseFound,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: 45,
                PayloadLength: uint.MaxValue,
                Reserved: 0);
            header.WriteTo(headerOnly);

            ListenerFrameParseResult result = ListenerProtocolParser.ParseOneFrame(headerOnly);

            Assert.Equal(ListenerFrameParseStatus.Incomplete, result.Status);
            Assert.Equal(0, result.ConsumedBytes);
        }

        [Fact]
        public void EncodeGetRequest_WhenMessageIdMd5Invalid_Throws()
        {
            Assert.Throws<ArgumentException>(() => ListenerProtocolEncoder.EncodeGetRequest(1, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"));
            Assert.Throws<ArgumentException>(() => ListenerProtocolEncoder.EncodeGetRequest(1, "30edc94157aa16fe644a45a1f1ffe16g"));
            Assert.Throws<ArgumentException>(() => ListenerProtocolEncoder.EncodeGetRequest(1, "short"));
        }

        [Fact]
        public void EncodeGetRequest_WhenValid_EncodesExpectedFrame()
        {
            const string messageIdMd5 = "30edc94157aa16fe644a45a1f1ffe160";
            byte[] frame = ListenerProtocolEncoder.EncodeGetRequest(55, messageIdMd5);

            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(frame);
            Assert.Equal(ListenerOpcode.GetRequest, header.Opcode);
            Assert.Equal(55U, header.RequestId);
            Assert.Equal(32U, header.PayloadLength);
            Assert.Equal(messageIdMd5, Encoding.ASCII.GetString(frame.AsSpan(ListenerProtocol.HeaderLengthBytes, 32)));
        }

        [Fact]
        public void CrcFieldsAreNotPresent_InV1FrameLayout()
        {
            byte[] frame = ListenerProtocolEncoder.EncodeGetReceiptAck(61);
            Assert.Equal(ListenerProtocol.HeaderLengthBytes, frame.Length);

            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(frame);
            Assert.Equal((ushort)16, header.HeaderLength);
            Assert.Equal(0U, header.PayloadLength);
        }

        private static ListenerFrameParseResult Parse(byte[] frame)
        {
            ReadOnlySequence<byte> sequence = new(frame);
            return ListenerProtocolParser.ParseOneFrame(in sequence);
        }

        private static byte[] CreateFrame(byte version, ListenerOpcode opcode, uint requestId, byte[] payload)
        {
            return CreateRawFrame(
                version,
                (byte)opcode,
                ListenerProtocol.HeaderLengthBytes,
                requestId,
                checked((uint)payload.Length),
                0,
                payload);
        }

        private static byte[] CreateRawFrame(
            byte version,
            byte opcode,
            ushort headerLength,
            uint requestId,
            uint payloadLength,
            uint reserved,
            byte[] payload)
        {
            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes + payload.Length];
            frame[0] = version;
            frame[1] = opcode;
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(2, 2), headerLength);
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(4, 4), requestId);
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(8, 4), payloadLength);
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(12, 4), reserved);
            payload.CopyTo(frame, ListenerProtocol.HeaderLengthBytes);
            return frame;
        }
    }
}
