// <copyright file="TransitProtocolParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit protocol parser behavior.

using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Text;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// NNTP protocol parsing helpers for greeting, status lines, multiline capability responses, and tokenization.
    /// </summary>
    internal static class TransitProtocolParser
    {
        /// <summary>
        /// Tracks capabilities response code for transit protocol parser.
        /// </summary>
        private const int CapabilitiesResponseCode = 101;
        /// <summary>
        /// Limits maximum nntp line length bytes for transit protocol parser.
        /// </summary>
        private const int MaximumNntpLineLengthBytes = 16 * 1024;

        /// <summary>
        /// Coordinates read nntp line async for transit protocol parser.
        /// </summary>
        internal static async ValueTask<string> ReadNntpLineAsync(PipeReader reader, CancellationToken cancellationToken)
        {
            (string? line, _, bool completedWithoutLine) = await ReadNntpLineWithByteCountAndCompletionAsync(reader, cancellationToken).ConfigureAwait(false);
            return completedWithoutLine ? throw new InvalidOperationException("NNTP connection closed while awaiting line response.") : line!;
        }

        /// <summary>
        /// Coordinates read nntp line with byte count async for transit protocol parser.
        /// </summary>
        internal static async ValueTask<(string Line, int BytesRead)> ReadNntpLineWithByteCountAsync(PipeReader reader, CancellationToken cancellationToken)
        {
            (string? line, int bytesRead, bool completedWithoutLine) = await ReadNntpLineWithByteCountAndCompletionAsync(reader, cancellationToken).ConfigureAwait(false);
            return completedWithoutLine
                ? throw new InvalidOperationException("NNTP connection closed while awaiting line response.")
                : ((string Line, int BytesRead))(line!, bytesRead);
        }

        /// <summary>
        /// Reads one NNTP protocol line and reports whether the underlying stream completed before a full line was available.
        /// </summary>
        /// <param name="reader">Pipe reader providing NNTP protocol bytes.</param>
        /// <param name="cancellationToken">Cancellation token for cooperative shutdown.</param>
        /// <returns>
        /// A tuple containing the decoded line when available, byte count consumed, and a completion marker indicating EOF before newline.
        /// </returns>
        internal static async ValueTask<(string? Line, int BytesRead, bool CompletedWithoutLine)> ReadNntpLineWithByteCountAndCompletionAsync(PipeReader reader, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(reader);

            while (true)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                SequencePosition? newLinePosition = buffer.PositionOf((byte)'\n');
                if (newLinePosition.HasValue)
                {
                    ReadOnlySequence<byte> line = buffer.Slice(buffer.Start, newLinePosition.Value);
                    string decodedLine = DecodeLine(line);
                    SequencePosition afterNewLine = buffer.GetPosition(1, newLinePosition.Value);
                    int bytesRead = checked((int)buffer.Slice(buffer.Start, afterNewLine).Length);
                    reader.AdvanceTo(afterNewLine);
                    return (decodedLine, bytesRead, CompletedWithoutLine: false);
                }

                if (result.IsCompleted)
                {
                    int bytesRead = checked((int)buffer.Length);
                    reader.AdvanceTo(buffer.End);
                    return (null, bytesRead, CompletedWithoutLine: true);
                }

                if (buffer.Length > MaximumNntpLineLengthBytes)
                {
                    reader.AdvanceTo(buffer.End);
                    throw new InvalidOperationException($"NNTP response line exceeded maximum length of {MaximumNntpLineLengthBytes} bytes.");
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }

        /// <summary>
        /// Coordinates static for transit protocol parser.
        /// </summary>
        internal static (int Code, string ResponseText, string[] Tokens) ParseStatusLine(string line)
        {
            (int code, string responseText) = ParseStatusCodeAndText(line);
            string[] tokens = string.IsNullOrWhiteSpace(responseText)
                ? []
                : responseText.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            return (code, responseText, tokens);
        }

        /// <summary>
        /// Coordinates static for transit protocol parser.
        /// </summary>
        internal static (int Code, string ResponseText) ParseStatusCodeAndText(string line)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                throw new InvalidOperationException("NNTP response line is empty.");
            }

            if (line.Length < 3 || !char.IsDigit(line[0]) || !char.IsDigit(line[1]) || !char.IsDigit(line[2]))
            {
                throw new InvalidOperationException($"Malformed NNTP response code: '{line}'.");
            }

            if (line.Length > 3 && line[3] != ' ')
            {
                throw new InvalidOperationException($"Malformed NNTP response separator: '{line}'.");
            }

            int code = int.Parse(line.AsSpan(0, 3), NumberStyles.None, CultureInfo.InvariantCulture);
            string responseText = line.Length > 4 ? line[4..] : string.Empty;
            return (code, responseText);
        }

        /// <summary>
        /// Coordinates validate greeting for transit protocol parser.
        /// </summary>
        internal static void ValidateGreeting(string greetingLine)
        {
            (int code, _, _) = ParseStatusLine(greetingLine);

            if (code is 200 or 201)
            {
                return;
            }

            throw new InvalidOperationException($"Unexpected NNTP greeting response code: {code}.");
        }

        /// <summary>
        /// Coordinates parse capabilities response for transit protocol parser.
        /// </summary>
        internal static TransitCapabilitySnapshot ParseCapabilitiesResponse(IReadOnlyList<string> responseLines)
        {
            ArgumentNullException.ThrowIfNull(responseLines);

            if (responseLines.Count < 2)
            {
                throw new InvalidOperationException("Malformed CAPABILITIES response: expected status line and terminator.");
            }

            (int responseCode, _, _) = ParseStatusLine(responseLines[0]);
            if (responseCode != CapabilitiesResponseCode)
            {
                throw new InvalidOperationException($"Unexpected CAPABILITIES response code: {responseCode}.");
            }

            if (!string.Equals(responseLines[^1], ".", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Malformed CAPABILITIES response: missing multiline terminator.");
            }

            bool supportsStartTls = false;
            bool supportsStreaming = false;

            for (int i = 1; i < responseLines.Count - 1; i++)
            {
                string capabilityLine = responseLines[i].Trim();
                if (capabilityLine.Length == 0)
                {
                    continue;
                }

                string[] tokens = capabilityLine.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (tokens.Length == 0)
                {
                    continue;
                }

                if (string.Equals(tokens[0], "STARTTLS", StringComparison.OrdinalIgnoreCase))
                {
                    supportsStartTls = true;
                    continue;
                }

                if (string.Equals(tokens[0], "STREAMING", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(tokens[0], "STREAM", StringComparison.OrdinalIgnoreCase))
                {
                    supportsStreaming = true;
                }
            }

            return new TransitCapabilitySnapshot(
                SupportsStartTls: supportsStartTls,
                SupportsStreaming: supportsStreaming);
        }

        /// <summary>
        /// Coordinates decode line for transit protocol parser.
        /// </summary>
        private static string DecodeLine(ReadOnlySequence<byte> line)
        {
            if (line.IsSingleSegment)
            {
                ReadOnlySpan<byte> span = line.FirstSpan;
                if (!span.IsEmpty && span[^1] == (byte)'\r')
                {
                    span = span[..^1];
                }

                return Encoding.ASCII.GetString(span);
            }

            int length = checked((int)line.Length);
            byte[] rented = ArrayPool<byte>.Shared.Rent(length);

            try
            {
                line.CopyTo(rented.AsSpan(0, length));

                int decodeLength = length;
                if (decodeLength > 0 && rented[decodeLength - 1] == (byte)'\r')
                {
                    decodeLength--;
                }

                return Encoding.ASCII.GetString(rented, 0, decodeLength);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: false);
            }
        }
    }
}
