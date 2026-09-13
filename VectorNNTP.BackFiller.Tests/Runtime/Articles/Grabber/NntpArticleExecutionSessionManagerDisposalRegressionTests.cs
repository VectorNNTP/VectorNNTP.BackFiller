// <copyright file="NntpArticleExecutionSessionManagerDisposalRegressionTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Grabber Tests
// Focused regression coverage for disposal retry semantics with multiple independently disposable sessions.

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Grabber;

/// <summary>
/// Regression tests for manager disposal retry semantics across multiple independently disposable sessions.
/// </summary>
public sealed class NntpArticleExecutionSessionManagerDisposalRegressionTests
{
    /// <summary>
    /// Verifies disposal continues attempting all independently disposable sessions when one session disposal fails.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_WhenOneSessionDisposalFails_StillAttemptsOtherSessionsAndRetriesFailedSession()
    {
        await using MultiConnectionFakeArticleServer server = await MultiConnectionFakeArticleServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(false);

        NntpAccountSnapshot account = new(
            EntryId: Guid.NewGuid(),
            Backbone: "TestBackbone",
            Hostname: "127.0.0.1",
            KeepAliveSeconds: 30,
            MaxConnections: 2,
            Password: string.Empty,
            Port: (ushort)server.Port,
            ServerId: 1,
            Username: string.Empty,
            UseSsl: false);

        ConcurrentDictionary<NntpArticleAcquisitionSession, int> attempts = new(SessionReferenceComparer.Instance);
        int injectedFailureCount = 0;
        int successfulDisposals = 0;

        await using NntpArticleExecutionSessionManager manager = new(
            NullLogger<NntpArticleExecutionSessionManager>.Instance,
            options: null,
            timeProvider: null,
            loggerFactory: null,
            serverCertificateValidationCallback: null,
            sessionDisposer: async session =>
            {
                int count = attempts.AddOrUpdate(session, 1, static (_, current) => current + 1);
                if (count == 1 && Interlocked.CompareExchange(ref injectedFailureCount, 1, 0) == 0)
                {
                    throw new InvalidOperationException("Injected disposal failure for one session.");
                }

                await session.DisposeAsync().ConfigureAwait(false);
                _ = Interlocked.Increment(ref successfulDisposals);
            });

        await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, manager.ActiveSessionCount);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.DisposeAsync().ConfigureAwait(false)).ConfigureAwait(false);

        await manager.DisposeAsync().ConfigureAwait(false);

        Assert.Equal(0, manager.ActiveSessionCount);
        Assert.Equal(1, Volatile.Read(ref injectedFailureCount));
        Assert.Equal(2, attempts.Count);
        Assert.Contains(2, attempts.Values);
        Assert.Contains(1, attempts.Values);
        Assert.Equal(2, Volatile.Read(ref successfulDisposals));
    }

    private sealed class MultiConnectionFakeArticleServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _shutdown = new();
        private readonly Task _acceptLoop;
        private readonly int _acceptConnectionCount;
        private readonly object _clientsGate = new();
        private readonly List<TcpClient> _activeClients = [];

        private MultiConnectionFakeArticleServer(TcpListener listener, int acceptConnectionCount)
        {
            _listener = listener;
            _acceptConnectionCount = acceptConnectionCount;
            _acceptLoop = Task.Run(AcceptLoopAsync);
        }

        internal static async Task<MultiConnectionFakeArticleServer> StartAsync(int acceptConnectionCount)
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            MultiConnectionFakeArticleServer server = new(listener, acceptConnectionCount);
            await Task.Delay(20).ConfigureAwait(false);
            return server;
        }

        internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        public async ValueTask DisposeAsync()
        {
            _shutdown.Cancel();
            try
            {
                _listener.Stop();
            }
            catch
            {
            }

            TcpClient[] clients;
            lock (_clientsGate)
            {
                clients = [.. _activeClients];
            }

            foreach (TcpClient client in clients)
            {
                try
                {
                    client.Dispose();
                }
                catch
                {
                }
            }

            await _acceptLoop.ConfigureAwait(false);
            _shutdown.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            List<Task> sessions = [];
            try
            {
                for (int i = 0; i < _acceptConnectionCount; i++)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                    sessions.Add(HandleClientAsync(client));
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }

            await Task.WhenAll(sessions).ConfigureAwait(false);
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            lock (_clientsGate)
            {
                _activeClients.Add(client);
            }

            using (client)
            using (NetworkStream stream = client.GetStream())
            {
                try
                {
                    await WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                    string? command = await ReadAsciiLineOrNullAsync(stream, _shutdown.Token).ConfigureAwait(false);
                    if (string.Equals(command, "QUIT", StringComparison.OrdinalIgnoreCase))
                    {
                        await WriteAsciiLineAsync(stream, "205 closing connection").ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (IOException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                finally
                {
                    lock (_clientsGate)
                    {
                        _ = _activeClients.Remove(client);
                    }
                }
            }
        }

        private static async Task<string?> ReadAsciiLineOrNullAsync(Stream stream, CancellationToken cancellationToken)
        {
            List<byte> bytes = [];
            byte[] single = new byte[1];

            while (true)
            {
                int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    return bytes.Count == 0 ? null : Encoding.ASCII.GetString([.. bytes]);
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

        private static async Task WriteAsciiLineAsync(Stream stream, string line)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
            await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
            await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private sealed class SessionReferenceComparer : IEqualityComparer<NntpArticleAcquisitionSession>
    {
        internal static readonly SessionReferenceComparer Instance = new();

        public bool Equals(NntpArticleAcquisitionSession? x, NntpArticleAcquisitionSession? y)
        {
            return ReferenceEquals(x, y);
        }

        public int GetHashCode(NntpArticleAcquisitionSession obj)
        {
            ArgumentNullException.ThrowIfNull(obj);
            return RuntimeHelpers.GetHashCode(obj);
        }
    }
}
