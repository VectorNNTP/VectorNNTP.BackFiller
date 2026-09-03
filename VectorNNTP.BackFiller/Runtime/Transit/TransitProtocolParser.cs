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
    /// Parses NNTP greeting, status, and CAPABILITIES responses from the transit transport pipeline.
    /// </summary>
    internal static class TransitProtocolParser
    {
        /// <summary>
        /// NNTP status code expected on a successful <c>CAPABILITIES</c> multiline response.
        /// </summary>
        private const int CapabilitiesResponseCode = 101;

        /// <summary>
        /// Maximum protocol line length accepted before the parser treats the response as malformed.
        /// </summary>
        private const int MaximumNntpLineLengthBytes = 16 * 1024;

        /// <summary>
        /// Reads one NNTP line and throws if the underlying stream reaches EOF before a full line is available.
        /// </summary>
        /// <param name="reader">Pipe reader supplying NNTP protocol bytes.</param>
        /// <param name="cancellationToken">Cancellation token for the read loop.</param>
        /// <returns>The decoded NNTP line without trailing CRLF.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="reader"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the stream completes before a full line is available.</exception>
        internal static async ValueTask<string> ReadNntpLineAsync(PipeReader reader, CancellationToken cancellationToken)
        {
            (string? line, _, bool completedWithoutLine) = await ReadNntpLineWithByteCountAndCompletionAsync(reader, cancellationToken).ConfigureAwait(false);
            return completedWithoutLine ? throw new InvalidOperationException("NNTP connection closed while awaiting line response.") : line!;
        }

        /// <summary>
        /// Reads one NNTP line and returns both the decoded text and the consumed byte count.
        /// </summary>
        /// <param name="reader">Pipe reader supplying NNTP protocol bytes.</param>
        /// <param name="cancellationToken">Cancellation token for the read loop.</param>
        /// <returns>The decoded NNTP line without trailing CRLF together with the number of consumed bytes.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="reader"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the stream completes before a full line is available.</exception>
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
        /// <returns>A tuple containing the decoded line, consumed byte count, and an EOF marker for completion before newline.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="reader"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when a protocol line exceeds <see cref="MaximumNntpLineLengthBytes"/>.</exception>
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
        /// Parses an NNTP status line into its numeric code, trailing text, and whitespace-delimited text tokens.
        /// </summary>
        /// <param name="line">NNTP status line to parse.</param>
        /// <returns>The parsed status code, trailing response text, and tokenized response text.</returns>
        internal static (int Code, string ResponseText, string[] Tokens) ParseStatusLine(string line)
        {
            (int code, string responseText) = ParseStatusCodeAndText(line);
            string[] tokens = string.IsNullOrWhiteSpace(responseText)
                ? []
                : responseText.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            return (code, responseText, tokens);
        }

        /// <summary>
        /// Parses and validates the three-digit status code and trailing text from an NNTP status line.
        /// </summary>
        /// <param name="line">NNTP status line to parse.</param>
        /// <returns>The parsed status code and trailing response text.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the line is empty or does not begin with a valid NNTP status code.</exception>
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
        /// Validates that the greeting line permits transit posting.
        /// </summary>
        /// <param name="greetingLine">Greeting line returned by the remote NNTP server.</param>
        /// <exception cref="InvalidOperationException">Thrown when the greeting code is not <c>200</c> or <c>201</c>.</exception>
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
        /// Parses an NNTP <c>CAPABILITIES</c> response and extracts the capability flags used by the transit pipeline.
        /// </summary>
        /// <param name="responseLines">Ordered response lines including the initial status line and terminating <c>.</c> line.</param>
        /// <returns>A capability snapshot describing STARTTLS and STREAMING support.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="responseLines"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the response is malformed or does not report status code <c>101</c>.</exception>
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
        /// Decodes an NNTP line from ASCII bytes, trimming a trailing carriage return when present.
        /// </summary>
        /// <param name="line">Sequence containing the line bytes up to but excluding the newline byte.</param>
        /// <returns>The decoded ASCII line text.</returns>
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
