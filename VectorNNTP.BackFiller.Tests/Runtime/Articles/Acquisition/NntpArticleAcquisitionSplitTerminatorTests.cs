// <copyright file="NntpArticleAcquisitionSplitTerminatorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Acquisition
// Deterministic regression coverage for oversized ARTICLE drain handling when the terminator is fragmented.

using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Verifies oversized ARTICLE cleanup handles a terminator fragmented across writes without waiting for the receive timeout.
    /// </summary>
    public sealed class NntpArticleAcquisitionSplitTerminatorTests
    {
        /// <summary>
        /// Verifies the split terminator is consumed by the oversized drain and does not leave the reader spinning on retained bytes.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenOversizedTerminatorIsSplit_CompletesDrainWithoutWaitingForReceiveTimeout()
        {
            const string messageId = "<split-terminator-regression@test>";
            byte[] headers = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                $"Message-ID: {messageId}\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "\r\n");

            int bodyLength = ArticleResourceLimits.MaxArticleBytes - headers.Length + 1;
            byte[] article = new byte[headers.Length + bodyLength];
            Buffer.BlockCopy(headers, 0, article, 0, headers.Length);
            Array.Fill(article, (byte)'A', headers.Length, bodyLength);
            article[^2] = (byte)'\r';
            article[^1] = (byte)'\n';

            await using SplitTerminatorServer server = await SplitTerminatorServer.StartAsync(messageId, article).ConfigureAwait(false);
            NntpArticleAcquisitionOptions options = NntpArticleAcquisitionOptions.Default with
            {
                ReceiveTimeout = TimeSpan.FromSeconds(30),
            };

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.Endpoint,
                options,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            using (connectResult)
            {
                await using (session)
                {
                    Task<NntpArticleAcquisitionResult> download = session.DownloadArticleAsync(messageId, CancellationToken.None).AsTask();
                    await server.FirstTerminatorFragmentWritten.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
                    Assert.False(download.IsCompleted);

                    server.ReleaseSecondTerminatorFragment();

                    NntpArticleAcquisitionResult result = await download.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                    using (result)
                    {
                        Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleTooLarge, result.FailureCode);
                    }
                }
            }

            await server.ConnectionClosed.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            Assert.True(server.SplitTerminatorWritten, "The test server must have written the terminator as two separate writes.");
            Assert.True(server.SecondTerminatorFragmentWritten, "The test server must have written the second terminator fragment after client-side drain was active.");
            Assert.Null(server.ServerFailure);
        }

        /// <summary>
        /// Minimal loopback server that deliberately writes the NNTP terminator as two separate transport writes.
        /// </summary>
        private sealed class SplitTerminatorServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly string _messageId;
            private readonly byte[] _article;
            private readonly CancellationTokenSource _shutdown = new();
            private readonly Task _acceptLoop;
            private readonly TaskCompletionSource<bool> _connectionClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _firstTerminatorFragmentWritten = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _releaseSecondTerminatorFragment = new(TaskCreationOptions.RunContinuationsAsynchronously);

            private SplitTerminatorServer(TcpListener listener, string messageId, byte[] article)
            {
                _listener = listener;
                _messageId = messageId;
                _article = article;
                Endpoint = new NntpArticleAcquisitionEndpoint(
                    "127.0.0.1",
                    ((IPEndPoint)listener.LocalEndpoint).Port,
                    UseSsl: false,
                    Username: null,
                    Password: null);
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            internal NntpArticleAcquisitionEndpoint Endpoint { get; }

            internal Task ConnectionClosed => _connectionClosed.Task;

            internal Task FirstTerminatorFragmentWritten => _firstTerminatorFragmentWritten.Task;

            internal bool SplitTerminatorWritten { get; private set; }

            internal bool SecondTerminatorFragmentWritten { get; private set; }

            internal Exception? ServerFailure { get; private set; }

            internal void ReleaseSecondTerminatorFragment() => _releaseSecondTerminatorFragment.TrySetResult(true);

            internal static ValueTask<SplitTerminatorServer> StartAsync(string messageId, byte[] article)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return ValueTask.FromResult(new SplitTerminatorServer(listener, messageId, article));
            }

            public async ValueTask DisposeAsync()
            {
                _shutdown.Cancel();
                _listener.Stop();
                try
                {
                    await _acceptLoop.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                {
                }

                _shutdown.Dispose();
            }

            private async Task AcceptLoopAsync()
            {
                try
                {
                    using TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                    await using NetworkStream stream = client.GetStream();

                    await WriteAsync(stream, "200 ready\r\n"u8.ToArray()).ConfigureAwait(false);
                    string command = await ReadLineAsync(stream, _shutdown.Token).ConfigureAwait(false);
                    Assert.Equal($"ARTICLE {_messageId}", command);

                    await WriteAsync(stream, Encoding.ASCII.GetBytes($"220 0 {_messageId} article follows\r\n")).ConfigureAwait(false);
                    await WriteAsync(stream, _article).ConfigureAwait(false);
                    await WriteAsync(stream, "."u8.ToArray()).ConfigureAwait(false);
                    SplitTerminatorWritten = true;
                    _firstTerminatorFragmentWritten.TrySetResult(true);

                    await _releaseSecondTerminatorFragment.Task.WaitAsync(TimeSpan.FromSeconds(5), _shutdown.Token).ConfigureAwait(false);
                    await WriteAsync(stream, "\r\n"u8.ToArray()).ConfigureAwait(false);
                    SecondTerminatorFragmentWritten = true;

                    byte[] singleByte = new byte[1];
                    while (true)
                    {
                        int read = await stream.ReadAsync(singleByte, _shutdown.Token).ConfigureAwait(false);
                        if (read == 0)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    ServerFailure = ex;
                }
                finally
                {
                    _connectionClosed.TrySetResult(true);
                }
            }

            private static async Task WriteAsync(NetworkStream stream, ReadOnlyMemory<byte> bytes)
            {
                await stream.WriteAsync(bytes).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
            }

            private static async Task<string> ReadLineAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                List<byte> bytes = [];
                byte[] single = new byte[1];
                while (true)
                {
                    int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException();
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

                return Encoding.ASCII.GetString(bytes.ToArray());
            }
        }
    }
}
