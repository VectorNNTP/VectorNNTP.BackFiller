// <copyright file="NntpArticleAcquisitionBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Benchmarks / Articles / Acquisition
// BenchmarkDotNet suite that measures steady-state ARTICLE throughput over reusable,
// pre-authenticated NNTP sessions with setup and teardown outside measured regions.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Measures steady-state loopback ARTICLE performance over reusable authenticated sessions.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 8)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    /// <summary>
    /// Represents the nntp ArticleAcquisitionBenchmarks class used by the benchmark or regression gate.
    /// </summary>
    public class NntpArticleAcquisitionBenchmarks
    {
        /// <summary>
        /// Number of ARTICLE operations executed in each measured benchmark iteration.
        /// </summary>
        private const int ArticleOperationsPerIteration = 100;

        /// <summary>
        /// Shared benchmark username used by loopback authentication flow.
        /// </summary>
        private const string BenchmarkUsername = "benchmark-user";

        /// <summary>
        /// Shared benchmark password used by loopback authentication flow.
        /// </summary>
        private const string BenchmarkPassword = "benchmark-pass";

        /// <summary>
        /// Shared acquisition options.
        /// </summary>
        private readonly NntpArticleAcquisitionOptions _options = NntpArticleAcquisitionOptions.Default;

        /// <summary>
        /// Loopback fake server for small article benchmark.
        /// </summary>
        private LoopbackArticleFixtureServer _smallServer = null!;

        /// <summary>
        /// Loopback fake server for typical article benchmark.
        /// </summary>
        private LoopbackArticleFixtureServer _typicalServer = null!;

        /// <summary>
        /// Loopback fake server for large article benchmark.
        /// </summary>
        private LoopbackArticleFixtureServer _largeServer = null!;

        /// <summary>
        /// Loopback fake server for large yEnc article benchmark.
        /// </summary>
        private LoopbackArticleFixtureServer _largeYEncServer = null!;

        /// <summary>
        /// Size of small benchmark article bytes.
        /// </summary>
        private int _smallArticleBytes;

        /// <summary>
        /// Size of typical benchmark article bytes.
        /// </summary>
        private int _typicalArticleBytes;

        /// <summary>
        /// Size of large benchmark article bytes.
        /// </summary>
        private int _largeArticleBytes;

        /// <summary>
        /// Size of large yEnc benchmark article bytes.
        /// </summary>
        private int _largeYEncArticleBytes;

        /// <summary>
        /// Connected and authenticated session used by the small article benchmark target.
        /// </summary>
        private NntpArticleAcquisitionSession? _smallSession;

        /// <summary>
        /// Connected and authenticated session used by the typical article benchmark target.
        /// </summary>
        private NntpArticleAcquisitionSession? _typicalSession;

        /// <summary>
        /// Connected and authenticated session used by the large yEnc article benchmark target.
        /// </summary>
        private NntpArticleAcquisitionSession? _largeYEncSession;

        /// <summary>
        /// Builds deterministic fixtures, starts loopback fake servers, and establishes reusable authenticated sessions.
        /// </summary>
        [GlobalSetup]
        /// <summary>
        /// Implements the setup Async contract.
        /// </summary>
        public async Task SetupAsync()
        {
            byte[] smallArticle = BuildArticleBytes("<bench-small@test>", BuildRepeatedTextLine("small", 16));
            byte[] typicalArticle = BuildArticleBytes("<bench-typical@test>", BuildRepeatedTextLine("typical", 1024));
            byte[] largeArticle = BuildArticleBytes("<bench-large@test>", BuildRepeatedTextLine("L", 131_072));
            byte[] largeYEncArticle = BuildArticleBytes("<bench-yenc@test>", BuildSyntheticYEncBody(2_097_152));

            _smallArticleBytes = smallArticle.Length;
            _typicalArticleBytes = typicalArticle.Length;
            _largeArticleBytes = largeArticle.Length;
            _largeYEncArticleBytes = largeYEncArticle.Length;

            _smallServer = await LoopbackArticleFixtureServer.StartAsync("<bench-small@test>", smallArticle, BenchmarkUsername, BenchmarkPassword).ConfigureAwait(false);
            _typicalServer = await LoopbackArticleFixtureServer.StartAsync("<bench-typical@test>", typicalArticle, BenchmarkUsername, BenchmarkPassword).ConfigureAwait(false);
            _largeServer = await LoopbackArticleFixtureServer.StartAsync("<bench-large@test>", largeArticle, BenchmarkUsername, BenchmarkPassword).ConfigureAwait(false);
            _largeYEncServer = await LoopbackArticleFixtureServer.StartAsync("<bench-yenc@test>", largeYEncArticle, BenchmarkUsername, BenchmarkPassword).ConfigureAwait(false);

            _smallSession = await ConnectSessionAsync(_smallServer.Endpoint).ConfigureAwait(false);
            _typicalSession = await ConnectSessionAsync(_typicalServer.Endpoint).ConfigureAwait(false);
            _largeYEncSession = await ConnectSessionAsync(_largeYEncServer.Endpoint).ConfigureAwait(false);
        }

        /// <summary>
        /// Stops reusable sessions and loopback servers.
        /// </summary>
        [GlobalCleanup]
        /// <summary>
        /// Implements the cleanup Async contract.
        /// </summary>
        public async Task CleanupAsync()
        {
            if (_smallSession is not null)
            {
                await _smallSession.DisposeAsync().ConfigureAwait(false);
                _smallSession = null;
            }

            if (_typicalSession is not null)
            {
                await _typicalSession.DisposeAsync().ConfigureAwait(false);
                _typicalSession = null;
            }

            if (_largeYEncSession is not null)
            {
                await _largeYEncSession.DisposeAsync().ConfigureAwait(false);
                _largeYEncSession = null;
            }

            await _smallServer.DisposeAsync().ConfigureAwait(false);
            await _typicalServer.DisposeAsync().ConfigureAwait(false);
            await _largeServer.DisposeAsync().ConfigureAwait(false);
            await _largeYEncServer.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Measures steady-state ARTICLE throughput for small article shape over one reusable authenticated session.
        /// </summary>
        /// <returns>Total bytes downloaded across the measured article operations.</returns>
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("SteadyStateArticle")]
        /// <summary>
        /// Implements the steady StateSmallArticleAsync contract.
        /// </summary>
        public Task<int> SteadyStateSmallArticleAsync()
        {
            return RunSteadyStateArticleLoopAsync(_smallSession, "<bench-small@test>");
        }

        /// <summary>
        /// Measures steady-state ARTICLE throughput for typical article shape over one reusable authenticated session.
        /// </summary>
        /// <returns>Total bytes downloaded across the measured article operations.</returns>
        [Benchmark]
        [BenchmarkCategory("SteadyStateArticle")]
        /// <summary>
        /// Implements the steady StateTypicalArticleAsync contract.
        /// </summary>
        public Task<int> SteadyStateTypicalArticleAsync()
        {
            return RunSteadyStateArticleLoopAsync(_typicalSession, "<bench-typical@test>");
        }

        /// <summary>
        /// Measures steady-state ARTICLE throughput for large yEnc-shaped article over one reusable authenticated session.
        /// </summary>
        /// <returns>Total bytes downloaded across the measured article operations.</returns>
        [Benchmark]
        [BenchmarkCategory("SteadyStateArticle")]
        /// <summary>
        /// Implements the steady StateLargey EncArticleAsync contract.
        /// </summary>
        public Task<int> SteadyStateLargeYEncArticleAsync()
        {
            return RunSteadyStateArticleLoopAsync(_largeYEncSession, "<bench-yenc@test>");
        }

        /// <summary>
        /// Gets configured ARTICLE operations per measured iteration.
        /// </summary>
        [BenchmarkCategory("Metadata")]
        /// <summary>
        /// Implements the article OperationsPerMeasuredIteration contract.
        /// </summary>
        public int ArticleOperationsPerMeasuredIteration()
        {
            return ArticleOperationsPerIteration;
        }

        /// <summary>
        /// Gets configured small article transfer size for throughput calculations.
        /// </summary>
        [BenchmarkCategory("Metadata")]
        /// <summary>
        /// Implements the small ArticleBytes contract.
        /// </summary>
        public int SmallArticleBytes()
        {
            return _smallArticleBytes;
        }

        /// <summary>
        /// Gets configured typical article transfer size for throughput calculations.
        /// </summary>
        [BenchmarkCategory("Metadata")]
        /// <summary>
        /// Implements the typical ArticleBytes contract.
        /// </summary>
        public int TypicalArticleBytes()
        {
            return _typicalArticleBytes;
        }

        /// <summary>
        /// Gets configured large article transfer size for throughput calculations.
        /// </summary>
        [BenchmarkCategory("Metadata")]
        /// <summary>
        /// Implements the large ArticleBytes contract.
        /// </summary>
        public int LargeArticleBytes()
        {
            return _largeArticleBytes;
        }

        /// <summary>
        /// Gets configured large yEnc article transfer size for throughput calculations.
        /// </summary>
        [BenchmarkCategory("Metadata")]
        /// <summary>
        /// Implements the large y EncArticleBytes contract.
        /// </summary>
        public int LargeYEncArticleBytes()
        {
            return _largeYEncArticleBytes;
        }

        /// <summary>
        /// Executes deterministic repeated ARTICLE operations against an already-connected authenticated session.
        /// </summary>
        /// <param name="session">Pre-connected and authenticated session instance.</param>
        /// <param name="messageId">Message-ID served by the benchmark fixture.</param>
        /// <returns>Total downloaded bytes across all operations in the iteration.</returns>
        private static async Task<int> RunSteadyStateArticleLoopAsync(NntpArticleAcquisitionSession? session, string messageId)
        {
            ArgumentNullException.ThrowIfNull(session);

            int totalBytes = 0;
            for (int i = 0; i < ArticleOperationsPerIteration; i++)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync(messageId, CancellationToken.None).ConfigureAwait(false);
                if (!result.IsSuccess)
                {
                    throw new InvalidOperationException($"Steady-state benchmark received non-success result {result.FailureCode} ({result.ResponseCode}) for {messageId}.");
                }

                totalBytes += result.ArticleLength;
            }

            return totalBytes;
        }

        /// <summary>
        /// Connects and authenticates one reusable acquisition session for benchmark steady-state measurement.
        /// </summary>
        /// <param name="endpoint">Benchmark server endpoint settings.</param>
        /// <returns>Connected and authenticated reusable acquisition session.</returns>
        private async Task<NntpArticleAcquisitionSession> ConnectSessionAsync(NntpArticleAcquisitionEndpoint endpoint)
        {
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                _options,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            return session is null
                ? throw new InvalidOperationException($"Failed to connect benchmark acquisition session: {connectResult.FailureCode} ({connectResult.ResponseCode}) {connectResult.ResponseText}")
                : session;
        }

        /// <summary>
        /// Builds deterministic NNTP article bytes with required parser headers.
        /// </summary>
        /// <param name="messageId">Message-ID header value.</param>
        /// <param name="body">Body payload.</param>
        /// <returns>Article bytes.</returns>
        private static byte[] BuildArticleBytes(string messageId, string body)
        {
            byte[] headerBytes = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                $"Message-ID: {messageId}\r\n" +
                "Newsgroups: alt.binaries.test\r\n" +
                "From: benchmark@example.test\r\n" +
                "\r\n");

            byte[] bodyBytes = Encoding.ASCII.GetBytes(body);
            byte[] article = new byte[headerBytes.Length + bodyBytes.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(bodyBytes, 0, article, headerBytes.Length, bodyBytes.Length);
            return article;
        }

        /// <summary>
        /// Builds repeated text body with CRLF line framing.
        /// </summary>
        /// <param name="line">Line text.</param>
        /// <param name="count">Line count.</param>
        /// <returns>Body text.</returns>
        private static string BuildRepeatedTextLine(string line, int count)
        {
            StringBuilder builder = new(count * (line.Length + 2));
            for (int i = 0; i < count; i++)
            {
                _ = builder.Append(line).Append("\r\n");
            }

            return builder.ToString();
        }

        /// <summary>
        /// Builds synthetic yEnc-like body for parser and acquisition stress.
        /// </summary>
        /// <param name="size">Decoded payload size.</param>
        /// <returns>Body text containing yEnc markers and encoded payload.</returns>
        private static string BuildSyntheticYEncBody(int size)
        {
            StringBuilder builder = new(size + 512);
            _ = builder.Append("=ybegin line=128 size=").Append(size).Append(" name=bench.bin\r\n");
            _ = builder.Append("=ypart begin=1 end=").Append(size).Append("\r\n");

            for (int i = 0; i < size; i++)
            {
                byte encoded = (byte)('!' + (i % 90));
                _ = builder.Append((char)encoded);
                if ((i + 1) % 128 == 0)
                {
                    _ = builder.Append("\r\n");
                }
            }

            if (!builder.ToString().EndsWith("\r\n", StringComparison.Ordinal))
            {
                _ = builder.Append("\r\n");
            }

            _ = builder.Append("=yend size=").Append(size).Append(" crc32=00000000\r\n");
            return builder.ToString();
        }

        /// <summary>
        /// Loopback fake server that authenticates once per connection and serves repeated ARTICLE commands.
        /// </summary>
        private sealed class LoopbackArticleFixtureServer : IAsyncDisposable
        {
            /// <summary>
            /// Listener backing this fixture server.
            /// </summary>
            private readonly TcpListener _listener;

            /// <summary>
            /// Served message identifier.
            /// </summary>
            private readonly string _messageId;

            /// <summary>
            /// Served article bytes.
            /// </summary>
            private readonly byte[] _article;

            /// <summary>
            /// Benchmark username expected by AUTHINFO USER.
            /// </summary>
            private readonly string _username;

            /// <summary>
            /// Benchmark password expected by AUTHINFO PASS.
            /// </summary>
            private readonly string _password;

            /// <summary>
            /// Loop cancellation source.
            /// </summary>
            private readonly CancellationTokenSource _cancellation = new();

            /// <summary>
            /// Accept loop task.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Initializes the loopback fixture server.
            /// </summary>
            /// <param name="listener">Started listener.</param>
            /// <param name="messageId">Served Message-ID.</param>
            /// <param name="article">Served article payload.</param>
            /// <param name="username">Expected benchmark username.</param>
            /// <param name="password">Expected benchmark password.</param>
            private LoopbackArticleFixtureServer(TcpListener listener, string messageId, byte[] article, string username, string password)
            {
                _listener = listener;
                _messageId = messageId;
                _article = article;
                _username = username;
                _password = password;
                _acceptLoop = Task.Run(AcceptLoopAsync);
                Endpoint = new NntpArticleAcquisitionEndpoint("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port, UseSsl: false, Username: username, Password: password);
            }

            /// <summary>
            /// Gets endpoint settings consumable by the acquisition session.
            /// </summary>
            internal NntpArticleAcquisitionEndpoint Endpoint { get; }

            /// <summary>
            /// Starts a loopback fixture server.
            /// </summary>
            /// <param name="messageId">Served Message-ID.</param>
            /// <param name="article">Served article bytes.</param>
            /// <param name="username">Expected benchmark username.</param>
            /// <param name="password">Expected benchmark password.</param>
            /// <returns>Started server.</returns>
            internal static async Task<LoopbackArticleFixtureServer> StartAsync(string messageId, byte[] article, string username, string password)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                LoopbackArticleFixtureServer server = new(listener, messageId, article, username, password);
                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Disposes listener and stop token.
            /// </summary>
            /// <returns>Completion task.</returns>
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

            /// <summary>
            /// Accepts incoming benchmark connections and serves configured article payload.
            /// </summary>
            /// <returns>Loop completion task.</returns>
            private async Task AcceptLoopAsync()
            {
                while (!_cancellation.IsCancellationRequested)
                {
                    TcpClient? client = null;
                    try
                    {
                        client = await _listener.AcceptTcpClientAsync(_cancellation.Token).ConfigureAwait(false);
                        await using NetworkStream stream = client.GetStream();

                        await WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                        await ExpectAsciiLineAsync(stream, $"AUTHINFO USER {_username}", _cancellation.Token).ConfigureAwait(false);
                        await WriteAsciiLineAsync(stream, "381 pass required").ConfigureAwait(false);
                        await ExpectAsciiLineAsync(stream, $"AUTHINFO PASS {_password}", _cancellation.Token).ConfigureAwait(false);
                        await WriteAsciiLineAsync(stream, "281 auth accepted").ConfigureAwait(false);

                        while (!_cancellation.IsCancellationRequested)
                        {
                            string command = await ReadAsciiLineAsync(stream, _cancellation.Token).ConfigureAwait(false);
                            if (string.Equals(command, $"ARTICLE {_messageId}", StringComparison.Ordinal))
                            {
                                await WriteAsciiLineAsync(stream, $"220 0 {_messageId} article follows").ConfigureAwait(false);
                                await stream.WriteAsync(_article, _cancellation.Token).ConfigureAwait(false);
                                await stream.WriteAsync(".\r\n"u8.ToArray(), _cancellation.Token).ConfigureAwait(false);
                                await stream.FlushAsync(_cancellation.Token).ConfigureAwait(false);
                                continue;
                            }

                            if (command.StartsWith("ARTICLE ", StringComparison.Ordinal))
                            {
                                await WriteAsciiLineAsync(stream, "430 no such article").ConfigureAwait(false);
                                continue;
                            }

                            await WriteAsciiLineAsync(stream, "500 command not recognized").ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (EndOfStreamException)
                    {
                    }
                    finally
                    {
                        client?.Dispose();
                    }
                }
            }

            /// <summary>
            /// Validates one expected ASCII protocol line.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="expected">Expected line text.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Completion task.</returns>
            private static async Task ExpectAsciiLineAsync(Stream stream, string expected, CancellationToken cancellationToken)
            {
                string line = await ReadAsciiLineAsync(stream, cancellationToken).ConfigureAwait(false);
                if (!string.Equals(line, expected, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Expected '{expected}' but received '{line}'.");
                }
            }

            /// <summary>
            /// Reads one protocol command line from stream.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Command line without CRLF.</returns>
            private static async Task<string> ReadAsciiLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> bytes = [];
                byte[] single = new byte[1];

                while (true)
                {
                    int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Unexpected EOF while reading benchmark protocol line.");
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

                return Encoding.ASCII.GetString([.. bytes]);
            }

            /// <summary>
            /// Writes one ASCII line with CRLF termination.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="line">Line text without CRLF.</param>
            /// <returns>Completion task.</returns>
            private static async Task WriteAsciiLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
