// <copyright file="M16BoundaryMeasurementRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Benchmarks / Articles
// Deterministic forensic measurement runner comparing early acquisition rejection against late parser-stage rejection.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Executes deterministic M16 comparison measurements for early acquisition rejection versus late parser rejection.
    /// </summary>
    internal static class M16BoundaryMeasurementRunner
    {
        /// <summary>
        /// Number of repeated measurements per scenario.
        /// </summary>
        private const int Iterations = 5;

        /// <summary>
        /// Extra parser-side bytes above the fixed 5 MiB limit used by late-reject scenario.
        /// </summary>
        private const int LateRejectOversizeBytes = 256 * 1024;

        /// <summary>
        /// Executes both measurement scenarios and writes a concise forensic report to stdout.
        /// </summary>
        /// <returns>A task that completes after reporting both scenarios.</returns>
        internal static async Task RunAsync()
        {
            byte[] oversizedArticle = BuildOversizedArticle(ArticleResourceLimits.MaxArticleBytes + LateRejectOversizeBytes);

            ScenarioAggregate earlyAggregate = new("EarlyRejectThroughAcquisitionPath");
            ScenarioAggregate lateAggregate = new("LateRejectAfterClientMaterialization");

            for (int i = 0; i < Iterations; i++)
            {
                ScenarioSample earlySample = await MeasureEarlyRejectAsync(oversizedArticle).ConfigureAwait(false);
                earlyAggregate.Add(earlySample);

                ScenarioSample lateSample = MeasureLateReject(oversizedArticle);
                lateAggregate.Add(lateSample);
            }

            Console.WriteLine("M16 Measurement Report");
            Console.WriteLine($"Iterations: {Iterations}");
            Console.WriteLine($"HardArticleLimitBytes: {ArticleResourceLimits.MaxArticleBytes}");
            Console.WriteLine($"OversizedInputBytes: {oversizedArticle.Length}");
            Console.WriteLine("MeasurementNote: Scenarios are intentionally non-comparative because the late path bypasses acquisition boundary enforcement.");
            Console.WriteLine();
            WriteAggregate(earlyAggregate);
            Console.WriteLine();
            WriteAggregate(lateAggregate);
        }

        /// <summary>
        /// Measures the early acquisition rejection path by driving one oversized ARTICLE response through the real acquisition session.
        /// </summary>
        private static async Task<ScenarioSample> MeasureEarlyRejectAsync(byte[] oversizedArticle)
        {
            await using CountingArticleServer server = await CountingArticleServer.StartAsync("<m16-early@test>", oversizedArticle).ConfigureAwait(false);
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.Endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            if (session is null)
            {
                throw new InvalidOperationException($"Measurement connect failed: {connectResult.FailureCode} ({connectResult.ResponseCode}) {connectResult.ResponseText}");
            }

            await using (session)
            {
                ForceFullGc();
                long allocatedBefore = GC.GetTotalAllocatedBytes(true);
                Stopwatch stopwatch = Stopwatch.StartNew();

                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<m16-early@test>", CancellationToken.None).ConfigureAwait(false);

                stopwatch.Stop();
                long allocatedAfter = GC.GetTotalAllocatedBytes(true);

                if (result.FailureCode != NntpArticleAcquisitionFailureCode.ArticleTooLarge)
                {
                    throw new InvalidOperationException($"Expected ArticleTooLarge from early-reject scenario, received {result.FailureCode}.");
                }

                return new ScenarioSample(
                    Elapsed: stopwatch.Elapsed,
                    AllocatedBytes: allocatedAfter - allocatedBefore,
                    ServerBytesWritten: server.TotalBytesWritten,
                    ObservedMaterializedPayloadBytes: 0,
                    ExpectedMaterializationThresholdBytes: ArticleResourceLimits.MaxArticleBytes,
                    Outcome: result.FailureCode.ToString(),
                    MeasurementPath: "Acquisition");
            }
        }

        /// <summary>
        /// Measures a controlled late-reject scenario where oversized bytes are first materialized and only then rejected by parser size guardrails.
        /// </summary>
        private static ScenarioSample MeasureLateReject(byte[] oversizedArticle)
        {
            NntpArticleParser parser = new("bf01.usenet.ninja");

            ForceFullGc();
            long allocatedBefore = GC.GetTotalAllocatedBytes(true);
            Stopwatch stopwatch = Stopwatch.StartNew();

            byte[] materialized = new byte[oversizedArticle.Length];
            Buffer.BlockCopy(oversizedArticle, 0, materialized, 0, oversizedArticle.Length);
            NntpArticleParseResult parseResult = parser.Parse(materialized);

            stopwatch.Stop();
            long allocatedAfter = GC.GetTotalAllocatedBytes(true);

            if (parseResult.FailureCode != NntpArticleParseFailureCode.ArticleTooLarge)
            {
                throw new InvalidOperationException($"Expected ArticleTooLarge from late-reject scenario, received {parseResult.FailureCode}.");
            }

            return new ScenarioSample(
                Elapsed: stopwatch.Elapsed,
                AllocatedBytes: allocatedAfter - allocatedBefore,
                ServerBytesWritten: 0,
                ObservedMaterializedPayloadBytes: materialized.Length,
                ExpectedMaterializationThresholdBytes: ArticleResourceLimits.MaxArticleBytes,
                Outcome: parseResult.FailureCode.ToString(),
                MeasurementPath: "ParserAfterMaterialization");
        }

        /// <summary>
        /// Builds one valid NNTP article that exceeds the configured hard payload limit by construction.
        /// </summary>
        private static byte[] BuildOversizedArticle(int totalBytes)
        {
            byte[] headerBytes = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                "Message-ID: <m16-measure@test>\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "\r\n");

            int bodyBytes = totalBytes - headerBytes.Length;
            if (bodyBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalBytes), "Total bytes must exceed fixed header size.");
            }

            byte[] article = new byte[totalBytes];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);

            int bodyStart = headerBytes.Length;
            int bodyLength = totalBytes - bodyStart;
            for (int i = bodyStart; i < article.Length; i++)
            {
                article[i] = (byte)'A';
            }

            if (bodyLength >= 2)
            {
                article[^2] = (byte)'\r';
                article[^1] = (byte)'\n';
            }

            return article;
        }

        /// <summary>
        /// Emits one aggregate scenario row.
        /// </summary>
        private static void WriteAggregate(ScenarioAggregate aggregate)
        {
            Console.WriteLine($"Scenario: {aggregate.Name}");
            Console.WriteLine($"  Outcome: {aggregate.LastOutcome}");
            Console.WriteLine($"  MeasurementPath: {aggregate.MeasurementPath}");
            Console.WriteLine($"  AvgElapsedMs: {aggregate.AverageElapsed.TotalMilliseconds:F2}");
            Console.WriteLine($"  AvgAllocatedBytes: {aggregate.AverageAllocatedBytes}");
            Console.WriteLine($"  AvgServerBytesWritten: {aggregate.AverageServerBytesWritten}");
            Console.WriteLine($"  AvgObservedMaterializedPayloadBytes: {aggregate.AverageObservedMaterializedPayloadBytes}");
            Console.WriteLine($"  AvgExpectedMaterializationThresholdBytes: {aggregate.AverageExpectedMaterializationThresholdBytes}");
        }

        /// <summary>
        /// Forces a full collection to reduce cross-scenario allocation noise.
        /// </summary>
        private static void ForceFullGc()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// One measured scenario sample.
        /// </summary>
        private readonly record struct ScenarioSample(
            TimeSpan Elapsed,
            long AllocatedBytes,
            long ServerBytesWritten,
            int ObservedMaterializedPayloadBytes,
            int ExpectedMaterializationThresholdBytes,
            string Outcome,
            string MeasurementPath);

        /// <summary>
        /// Aggregate metrics for one scenario across repeated samples.
        /// </summary>
        private sealed class ScenarioAggregate
        {
            private readonly List<ScenarioSample> _samples = [];

            internal ScenarioAggregate(string name)
            {
                Name = name;
            }

            internal string Name { get; }

            internal string LastOutcome => _samples.Count == 0 ? "n/a" : _samples[^1].Outcome;

            internal string MeasurementPath => _samples.Count == 0 ? "n/a" : _samples[^1].MeasurementPath;

            internal TimeSpan AverageElapsed => TimeSpan.FromTicks((long)(_samples.Count == 0 ? 0d : _samples.Average(static sample => (double)sample.Elapsed.Ticks)));

            internal long AverageAllocatedBytes => (long)(_samples.Count == 0 ? 0d : _samples.Average(static sample => (double)sample.AllocatedBytes));

            internal long AverageServerBytesWritten => (long)(_samples.Count == 0 ? 0d : _samples.Average(static sample => (double)sample.ServerBytesWritten));

            internal int AverageObservedMaterializedPayloadBytes => (int)(_samples.Count == 0 ? 0d : _samples.Average(static sample => (double)sample.ObservedMaterializedPayloadBytes));

            internal int AverageExpectedMaterializationThresholdBytes => (int)(_samples.Count == 0 ? 0d : _samples.Average(static sample => (double)sample.ExpectedMaterializationThresholdBytes));

            internal void Add(ScenarioSample sample)
            {
                _samples.Add(sample);
            }
        }

        /// <summary>
        /// Minimal loopback server that serves one oversized ARTICLE and counts bytes written on wire.
        /// </summary>
        private sealed class CountingArticleServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly string _messageId;
            private readonly byte[] _article;
            private readonly CancellationTokenSource _cancellation = new();
            private readonly Task _acceptLoop;

            private long _totalBytesWritten;

            private CountingArticleServer(TcpListener listener, string messageId, byte[] article)
            {
                _listener = listener;
                _messageId = messageId;
                _article = article;
                Endpoint = new NntpArticleAcquisitionEndpoint("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port, UseSsl: false, Username: null, Password: null);
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            internal NntpArticleAcquisitionEndpoint Endpoint { get; }

            internal long TotalBytesWritten => Interlocked.Read(ref _totalBytesWritten);

            internal static ValueTask<CountingArticleServer> StartAsync(string messageId, byte[] article)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return ValueTask.FromResult(new CountingArticleServer(listener, messageId, article));
            }

            public async ValueTask DisposeAsync()
            {
                _cancellation.Cancel();
                try
                {
                    _listener.Stop();
                }
                catch
                {
                }

                await _acceptLoop.ConfigureAwait(false);
                _cancellation.Dispose();
            }

            private async Task AcceptLoopAsync()
            {
                TcpClient? client = null;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(_cancellation.Token).ConfigureAwait(false);
                    await using NetworkStream stream = client.GetStream();

                    await WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                    string command = await ReadAsciiLineAsync(stream, _cancellation.Token).ConfigureAwait(false);
                    if (!string.Equals(command, $"ARTICLE {_messageId}", StringComparison.Ordinal))
                    {
                        await WriteAsciiLineAsync(stream, "500 unexpected command").ConfigureAwait(false);
                        return;
                    }

                    await WriteAsciiLineAsync(stream, $"220 0 {_messageId} article follows").ConfigureAwait(false);
                    await WriteBytesAsync(stream, _article).ConfigureAwait(false);
                    await WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
                {
                }
                finally
                {
                    client?.Dispose();
                }
            }

            private async Task WriteAsciiLineAsync(NetworkStream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await WriteBytesAsync(stream, bytes).ConfigureAwait(false);
            }

            private async Task WriteBytesAsync(NetworkStream stream, byte[] bytes)
            {
                await stream.WriteAsync(bytes, _cancellation.Token).ConfigureAwait(false);
                await stream.FlushAsync(_cancellation.Token).ConfigureAwait(false);
                _ = Interlocked.Add(ref _totalBytesWritten, bytes.Length);
            }

            private static async Task<string> ReadAsciiLineAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                List<byte> bytes = [];
                byte[] single = new byte[1];

                while (true)
                {
                    int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Unexpected EOF while reading command.");
                    }

                    if (single[0] == (byte)'\n')
                    {
                        break;
                    }

                    bytes.Add(single[0]);
                }

                if (bytes.Count > 0 && bytes[^1] == (byte)'\r')
                {
                    bytes.RemoveAt(bytes.Count - 1);
                }

                return Encoding.ASCII.GetString(CollectionsMarshal.AsSpan(bytes));
            }
        }
    }
}
