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
        private const int CapabilitiesResponseCode = 101;
        private const int MaximumNntpLineLengthBytes = 16 * 1024;

        internal static async ValueTask<string> ReadNntpLineAsync(PipeReader reader, CancellationToken cancellationToken)
        {
            (string line, _) = await ReadNntpLineWithByteCountAsync(reader, cancellationToken).ConfigureAwait(false);
            return line;
        }

        internal static async ValueTask<(string Line, int BytesRead)> ReadNntpLineWithByteCountAsync(PipeReader reader, CancellationToken cancellationToken)
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
                    return (decodedLine, bytesRead);
                }

                if (result.IsCompleted)
                {
                    reader.AdvanceTo(buffer.End);
                    throw new InvalidOperationException("NNTP connection closed while awaiting line response.");
                }

                if (buffer.Length > MaximumNntpLineLengthBytes)
                {
                    reader.AdvanceTo(buffer.End);
                    throw new InvalidOperationException($"NNTP response line exceeded maximum length of {MaximumNntpLineLengthBytes} bytes.");
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }

        internal static (int Code, string ResponseText, string[] Tokens) ParseStatusLine(string line)
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
            string[] tokens = string.IsNullOrWhiteSpace(responseText)
                ? []
                : responseText.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            return (code, responseText, tokens);
        }

        internal static void ValidateGreeting(string greetingLine)
        {
            (int code, _, _) = ParseStatusLine(greetingLine);

            if (code is 200 or 201)
            {
                return;
            }

            throw new InvalidOperationException($"Unexpected NNTP greeting response code: {code}.");
        }

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
            bool supportsCompressDeflate = false;
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

                if (tokens.Length >= 2
                    && string.Equals(tokens[0], "COMPRESS", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(tokens[1], "DEFLATE", StringComparison.OrdinalIgnoreCase))
                {
                    supportsCompressDeflate = true;
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
                SupportsCompressDeflate: supportsCompressDeflate,
                SupportsStreaming: supportsStreaming);
        }

        private static string DecodeLine(ReadOnlySequence<byte> line)
        {
            byte[] lineBytes = line.ToArray();

            int length = lineBytes.Length;
            if (length > 0 && lineBytes[length - 1] == (byte)'\r')
            {
                length--;
            }

            return Encoding.ASCII.GetString(lineBytes, 0, length);
        }
    }
}
