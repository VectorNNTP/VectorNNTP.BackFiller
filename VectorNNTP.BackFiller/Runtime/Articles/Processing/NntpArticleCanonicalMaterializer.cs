// <copyright file="NntpArticleCanonicalMaterializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Post-validation article materialization that rewrites canonical Date and Path header values
// before retention ownership transfer.

using System.Buffers;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Represents deterministic canonical materialization rejection caused by hard article or line resource boundaries.
    /// </summary>
    internal sealed class NntpArticleCanonicalBoundaryException : InvalidOperationException
    {
        /// <summary>
        /// Initializes a new exception instance with the specified message.
        /// </summary>
        /// <param name="message">Boundary-rejection diagnostic message.</param>
        internal NntpArticleCanonicalBoundaryException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Materializes one validated article into the canonical retained-byte representation used by retention, Transit, and Listener.
    /// </summary>
    /// <remarks>
    /// The materializer rewrites only the selected date-header value and the Path header value while preserving all unrelated header bytes and body bytes exactly.
    /// It always constructs a new pooled payload owner so ownership transfer into retention remains explicit and does not mutate shared parser input slices.
    /// </remarks>
    internal static class NntpArticleCanonicalMaterializer
    {
        /// <summary>
        /// Builds a canonicalized article payload from a parser-accepted article.
        /// </summary>
        /// <param name="parseResult">Accepted parse result carrying canonical Path/Date metadata and source-byte header slices.</param>
        /// <returns>A newly owned pooled buffer containing the canonical retained payload bytes.</returns>
        /// <exception cref="ArgumentException">Thrown when the parse result is not accepted.</exception>
        /// <exception cref="InvalidOperationException">Thrown when required date/path rewrite boundaries cannot be resolved from parser output.</exception>
        internal static DownloadedArticleBuffer Materialize(NntpArticleParseResult parseResult)
        {
            if (!parseResult.IsAccepted)
            {
                throw new ArgumentException("Canonical article materialization requires an accepted parse result.", nameof(parseResult));
            }

            ReadOnlySpan<byte> source = parseResult.ArticleBytes.Span;
            if (source.IsEmpty)
            {
                throw new InvalidOperationException("Canonical article materialization requires non-empty article bytes.");
            }

            NntpArticleHeaderEntry dateHeader = ResolveSelectedDateHeader(parseResult, source);
            NntpArticleHeaderEntry? pathHeader = ResolvePathHeader(parseResult.Headers);

            byte[] canonicalDateBytes = Encoding.ASCII.GetBytes(parseResult.CanonicalUtcDate);
            byte[] canonicalPathBytes = Encoding.ASCII.GetBytes(parseResult.CanonicalPath);

            HeaderEdit dateEdit = new(dateHeader.ValueOffset, dateHeader.ValueLength, canonicalDateBytes);
            HeaderEdit? pathEdit = pathHeader.HasValue
                ? new HeaderEdit(pathHeader.Value.ValueOffset, pathHeader.Value.ValueLength, canonicalPathBytes)
                : null;

            HeaderInsert? pathInsert = null;
            if (!pathHeader.HasValue)
            {
                HeaderSeparator separator = ResolveHeaderSeparator(source, parseResult.HeaderBytes.Length);

                int lineLength = "Path: "u8.Length + canonicalPathBytes.Length + separator.LineTerminator.Length;
                byte[] lineBytes = new byte[lineLength];
                int offset = 0;
                "Path: "u8.CopyTo(lineBytes.AsSpan(offset));
                offset += "Path: "u8.Length;
                canonicalPathBytes.AsSpan().CopyTo(lineBytes.AsSpan(offset));
                offset += canonicalPathBytes.Length;
                separator.LineTerminator.CopyTo(lineBytes.AsSpan(offset));
                pathInsert = new HeaderInsert(separator.StartOffset, lineBytes, separator.LineTerminator.Length);
            }

            int lengthDelta = canonicalDateBytes.Length - dateEdit.RemovedLength;
            if (pathEdit.HasValue)
            {
                lengthDelta += pathEdit.Value.Replacement.Length - pathEdit.Value.RemovedLength;
            }

            if (pathInsert.HasValue)
            {
                lengthDelta += pathInsert.Value.Inserted.Length;
            }

            int destinationLength = checked(source.Length + lengthDelta);
            ValidateCanonicalArticleBoundaries(source, parseResult, dateEdit, pathEdit, pathInsert, destinationLength);
            byte[] rented = ArrayPool<byte>.Shared.Rent(destinationLength);
            int written = 0;
            int consumed = 0;

            try
            {
                Span<byte> destination = rented.AsSpan(0, destinationLength);

                if (pathEdit is HeaderEdit resolvedPathEdit && resolvedPathEdit.StartOffset < dateEdit.StartOffset)
                {
                    ApplyEdit(source, destination, ref consumed, ref written, resolvedPathEdit);
                    ApplyEdit(source, destination, ref consumed, ref written, dateEdit);
                }
                else
                {
                    ApplyEdit(source, destination, ref consumed, ref written, dateEdit);
                    if (pathEdit is HeaderEdit resolvedPathEditAfter)
                    {
                        ApplyEdit(source, destination, ref consumed, ref written, resolvedPathEditAfter);
                    }
                }

                if (pathInsert is HeaderInsert insert)
                {
                    ApplyInsert(source, destination, ref consumed, ref written, insert);
                }

                source[consumed..].CopyTo(destination[written..]);
                written += source.Length - consumed;

                return written == destinationLength
                    ? new DownloadedArticleBuffer(rented, destinationLength)
                    : throw new InvalidOperationException($"Canonical article materialization wrote {written} bytes but expected {destinationLength} bytes.");
            }
            catch
            {
                ArrayPool<byte>.Shared.Return(rented);
                throw;
            }
        }

        /// <summary>
        /// Resolves the exact parsed date header entry selected during date canonicalization.
        /// </summary>
        /// <param name="parseResult">Accepted parse result providing selected date metadata and header entries.</param>
        /// <param name="source">Original article bytes corresponding to <paramref name="parseResult"/>.</param>
        /// <returns>The header entry whose value must be replaced by <see cref="NntpArticleParseResult.CanonicalUtcDate"/>.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no matching selected date header entry can be resolved.</exception>
        private static NntpArticleHeaderEntry ResolveSelectedDateHeader(NntpArticleParseResult parseResult, ReadOnlySpan<byte> source)
        {
            if (parseResult.SelectedDateHeaderName == NntpArticleHeaderName.Unknown)
            {
                throw new InvalidOperationException("Accepted parse result did not specify which date header produced the canonical UTC date.");
            }

            ReadOnlySpan<byte> originalDateValue = parseResult.OriginalDateValue.Span;
            for (int i = 0; i < parseResult.Headers.Count; i++)
            {
                NntpArticleHeaderEntry header = parseResult.Headers[i];
                if (header.KnownName != parseResult.SelectedDateHeaderName)
                {
                    continue;
                }

                if (header.ValueLength != originalDateValue.Length)
                {
                    continue;
                }

                if (source.Slice(header.ValueOffset, header.ValueLength).SequenceEqual(originalDateValue))
                {
                    return header;
                }
            }

            throw new InvalidOperationException("Accepted parse result did not contain a resolvable selected date header/value pair for canonical rewrite.");
        }

        /// <summary>
        /// Resolves the optional parsed Path header entry.
        /// </summary>
        /// <param name="headers">Parsed article headers in wire order.</param>
        /// <returns>The unique Path header entry when present; otherwise <see langword="null"/>.</returns>
        private static NntpArticleHeaderEntry? ResolvePathHeader(IReadOnlyList<NntpArticleHeaderEntry> headers)
        {
            NntpArticleHeaderEntry? pathHeader = null;
            for (int i = 0; i < headers.Count; i++)
            {
                NntpArticleHeaderEntry header = headers[i];
                if (header.KnownName != NntpArticleHeaderName.Path)
                {
                    continue;
                }

                if (pathHeader.HasValue)
                {
                    throw new InvalidOperationException("Accepted parse result contained duplicate Path headers during canonical materialization.");
                }

                pathHeader = header;
            }

            return pathHeader;
        }

        /// <summary>
        /// Resolves the header/body separator boundary and line-terminator style preserved by parser-accepted header bytes.
        /// </summary>
        /// <param name="source">Complete original article bytes.</param>
        /// <param name="headerLength">Header-section byte length from parser output.</param>
        /// <returns>Separator metadata used for Path insertion while preserving source separator style.</returns>
        /// <exception cref="InvalidOperationException">Thrown when header bytes do not end with an accepted blank-line terminator sequence.</exception>
        private static HeaderSeparator ResolveHeaderSeparator(ReadOnlySpan<byte> source, int headerLength)
        {
            _ = headerLength <= 0 || headerLength > source.Length
                ? throw new InvalidOperationException("Accepted parse result does not contain a valid header section boundary for Path insertion.")
                : 0;

            return headerLength >= 4
                && source[headerLength - 4] == (byte)'\r'
                && source[headerLength - 3] == (byte)'\n'
                && source[headerLength - 2] == (byte)'\r'
                && source[headerLength - 1] == (byte)'\n'
                ? new HeaderSeparator(headerLength - 2, "\r\n"u8.ToArray())
                : headerLength >= 2
                && source[headerLength - 2] == (byte)'\n'
                && source[headerLength - 1] == (byte)'\n'
                ? new HeaderSeparator(headerLength - 1, "\n"u8.ToArray())
                : headerLength >= 2
                && source[headerLength - 2] == (byte)'\r'
                && source[headerLength - 1] == (byte)'\r'
                ? new HeaderSeparator(headerLength - 1, "\r"u8.ToArray())
                : throw new InvalidOperationException("Accepted parse result header section does not terminate with an accepted separator style and cannot be safely materialized.");
        }

        /// <summary>
        /// Validates canonical output boundaries after deterministic Date/Path rewrite planning and before destination rental.
        /// </summary>
        /// <param name="source">Original article bytes.</param>
        /// <param name="parseResult">Accepted parser output used for canonicalization metadata.</param>
        /// <param name="dateEdit">Resolved Date rewrite.</param>
        /// <param name="pathEdit">Resolved Path rewrite when Path exists.</param>
        /// <param name="pathInsert">Resolved Path insertion when Path is missing.</param>
        /// <param name="destinationLength">Deterministic canonical destination length.</param>
        /// <exception cref="InvalidOperationException">Thrown when canonical output would exceed hard article or line boundaries.</exception>
        private static void ValidateCanonicalArticleBoundaries(
            ReadOnlySpan<byte> source,
            NntpArticleParseResult parseResult,
            HeaderEdit dateEdit,
            HeaderEdit? pathEdit,
            HeaderInsert? pathInsert,
            int destinationLength)
        {
            if (destinationLength > ArticleResourceLimits.MaxArticleBytes)
            {
                throw new NntpArticleCanonicalBoundaryException(
                    $"Canonical article materialization would produce {destinationLength} bytes, exceeding hard maximum {ArticleResourceLimits.MaxArticleBytes} bytes.");
            }

            int selectedDateLineLength = ComputePhysicalHeaderLineLength(
                source,
                dateEdit.StartOffset,
                dateEdit.RemovedLength,
                dateEdit.Replacement.Length,
                parseResult.Headers,
                parseResult.HeaderBytes.Length);
            if (selectedDateLineLength > ArticleResourceLimits.MaxArticleLineBytes)
            {
                throw new NntpArticleCanonicalBoundaryException(
                    $"Canonical date rewrite would produce a physical header content length of {selectedDateLineLength} bytes, exceeding hard maximum {ArticleResourceLimits.MaxArticleLineBytes} bytes.");
            }

            if (pathEdit is HeaderEdit resolvedPathEdit)
            {
                int rewrittenPathLineLength = ComputePhysicalHeaderLineLength(
                    source,
                    resolvedPathEdit.StartOffset,
                    resolvedPathEdit.RemovedLength,
                    resolvedPathEdit.Replacement.Length,
                    parseResult.Headers,
                    parseResult.HeaderBytes.Length);
                if (rewrittenPathLineLength > ArticleResourceLimits.MaxArticleLineBytes)
                {
                    throw new NntpArticleCanonicalBoundaryException(
                        $"Canonical Path rewrite would produce a physical header line content length of {rewrittenPathLineLength} bytes, exceeding hard maximum {ArticleResourceLimits.MaxArticleLineBytes} bytes.");
                }
            }

            if (pathInsert is HeaderInsert resolvedPathInsert)
            {
                int insertedPathLineContentLength = resolvedPathInsert.Inserted.Length - resolvedPathInsert.LineTerminatorLength;
                if (insertedPathLineContentLength > ArticleResourceLimits.MaxArticleLineBytes)
                {
                    throw new NntpArticleCanonicalBoundaryException(
                        $"Canonical Path insertion would produce a physical header line content length of {insertedPathLineContentLength} bytes, exceeding hard maximum {ArticleResourceLimits.MaxArticleLineBytes} bytes.");
                }
            }
        }

        /// <summary>
        /// Computes one physical header line length in bytes after replacing the parsed header value segment.
        /// </summary>
        /// <param name="source">Original article bytes.</param>
        /// <param name="valueOffset">Offset of replaced value start.</param>
        /// <param name="removedLength">Length of replaced value bytes in original header line.</param>
        /// <param name="replacementLength">Length of replacement value bytes.</param>
        /// <param name="headers">Parsed headers in wire order.</param>
        /// <param name="headerSectionLength">Parsed header-section length used to resolve the last header line boundary.</param>
        /// <returns>Physical header line content length in bytes excluding trailing line terminator framing.</returns>
        private static int ComputePhysicalHeaderLineLength(
            ReadOnlySpan<byte> source,
            int valueOffset,
            int removedLength,
            int replacementLength,
            IReadOnlyList<NntpArticleHeaderEntry> headers,
            int headerSectionLength)
        {
            int headerIndex = -1;
            for (int i = 0; i < headers.Count; i++)
            {
                if (headers[i].ValueOffset == valueOffset && headers[i].ValueLength == removedLength)
                {
                    headerIndex = i;
                    break;
                }
            }

            if (headerIndex < 0)
            {
                throw new InvalidOperationException("Canonical materialization could not resolve rewritten header boundary for line-length validation.");
            }

            int lineStart = headers[headerIndex].NameOffset;
            int lineEndExclusive = headerIndex + 1 < headers.Count
                ? headers[headerIndex + 1].NameOffset
                : ResolveHeaderSeparator(source, headerSectionLength).StartOffset;

            int originalLineLength = lineEndExclusive - lineStart;
            int lineTerminatorLength = ResolveTrailingLineTerminatorLength(source, lineStart, lineEndExclusive);
            int originalLineContentLength = originalLineLength - lineTerminatorLength;
            return checked(originalLineContentLength - removedLength + replacementLength);
        }

        /// <summary>
        /// Resolves the trailing line-terminator length for one physical header line.
        /// </summary>
        /// <param name="source">Original article bytes.</param>
        /// <param name="lineStart">Inclusive line-start offset.</param>
        /// <param name="lineEndExclusive">Exclusive line-end offset.</param>
        /// <returns>Trailing line-terminator length in bytes (<c>0</c>, <c>1</c>, or <c>2</c>).</returns>
        private static int ResolveTrailingLineTerminatorLength(ReadOnlySpan<byte> source, int lineStart, int lineEndExclusive)
        {
            int physicalLength = lineEndExclusive - lineStart;
            return physicalLength <= 0
                ? 0
                : physicalLength >= 2
                && source[lineEndExclusive - 2] == (byte)'\r'
                && source[lineEndExclusive - 1] == (byte)'\n'
                ? 2
                : source[lineEndExclusive - 1] is (byte)'\r' or (byte)'\n' ? 1 : 0;
        }

        /// <summary>
        /// Copies source bytes up to one replacement boundary and emits replacement bytes.
        /// </summary>
        /// <param name="source">Original article bytes.</param>
        /// <param name="destination">Destination canonical byte span.</param>
        /// <param name="consumed">Source bytes already consumed by prior edits.</param>
        /// <param name="written">Destination bytes already written by prior edits.</param>
        /// <param name="edit">Replacement boundary and replacement bytes.</param>
        private static void ApplyEdit(ReadOnlySpan<byte> source, Span<byte> destination, ref int consumed, ref int written, HeaderEdit edit)
        {
            if (edit.StartOffset < consumed)
            {
                throw new InvalidOperationException("Canonical materialization encountered overlapping header rewrite ranges.");
            }

            source[consumed..edit.StartOffset].CopyTo(destination[written..]);
            written += edit.StartOffset - consumed;
            edit.Replacement.AsSpan().CopyTo(destination[written..]);
            written += edit.Replacement.Length;
            consumed = edit.StartOffset + edit.RemovedLength;
        }

        /// <summary>
        /// Copies source bytes up to one insertion boundary and emits inserted bytes without removing source bytes.
        /// </summary>
        /// <param name="source">Original article bytes.</param>
        /// <param name="destination">Destination canonical byte span.</param>
        /// <param name="consumed">Source bytes already consumed by prior edits.</param>
        /// <param name="written">Destination bytes already written by prior edits.</param>
        /// <param name="insert">Insertion boundary and inserted bytes.</param>
        private static void ApplyInsert(ReadOnlySpan<byte> source, Span<byte> destination, ref int consumed, ref int written, HeaderInsert insert)
        {
            if (insert.Offset < consumed)
            {
                throw new InvalidOperationException("Canonical materialization insertion offset overlaps a prior rewrite boundary.");
            }

            source[consumed..insert.Offset].CopyTo(destination[written..]);
            written += insert.Offset - consumed;
            insert.Inserted.AsSpan().CopyTo(destination[written..]);
            written += insert.Inserted.Length;
            consumed = insert.Offset;
        }

        /// <summary>
        /// Describes one replacement edit over the source article bytes.
        /// </summary>
        /// <param name="StartOffset">Inclusive source offset where replacement begins.</param>
        /// <param name="RemovedLength">Number of source bytes removed from the replacement range.</param>
        /// <param name="Replacement">Replacement bytes written at <paramref name="StartOffset"/>.</param>
        private readonly record struct HeaderEdit(int StartOffset, int RemovedLength, byte[] Replacement);

        /// <summary>
        /// Describes header/body separator metadata used for style-preserving Path insertion.
        /// </summary>
        /// <param name="StartOffset">Start offset of the blank-line terminator that begins the body boundary.</param>
        /// <param name="LineTerminator">Header line terminator style to preserve for inserted lines.</param>
        private readonly record struct HeaderSeparator(int StartOffset, byte[] LineTerminator);

        /// <summary>
        /// Describes one insertion edit over the source article bytes.
        /// </summary>
        /// <param name="Offset">Source offset where inserted bytes are emitted.</param>
        /// <param name="Inserted">Inserted bytes emitted at <paramref name="Offset"/> without consuming source bytes.</param>
        /// <param name="LineTerminatorLength">Trailing inserted line terminator length in bytes.</param>
        private readonly record struct HeaderInsert(int Offset, byte[] Inserted, int LineTerminatorLength);
    }
}
