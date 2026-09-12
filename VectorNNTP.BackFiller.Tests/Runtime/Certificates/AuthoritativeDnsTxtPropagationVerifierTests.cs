// <copyright file="AuthoritativeDnsTxtPropagationVerifierTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for authoritative DNS TXT propagation verifier, covering fallback, timeout, quorum, and cancellation behavior.

using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;
using Xunit.Sdk;

namespace VectorNNTP.BackFiller.Tests.Runtime.Certificates
{
    /// <summary>
    /// Verifies authoritative DNS TXT propagation verifier behavior.
    /// </summary>
    public sealed class AuthoritativeDnsTxtPropagationVerifierTests
    {
        /// <summary>
        /// Verifies resolver fallback continues after internal timeout and uses a later healthy resolver.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenFirstResolverTimesOut_UsesNextResolverAndSucceeds()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";
            IPAddress authorityAddress = IPAddress.Parse("203.0.113.10");

            int resolver1Attempts = 0;
            int resolver2Attempts = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.1")))
                    {
                        resolver1Attempts++;
                        throw new TimeoutException("simulated resolver timeout");
                    }

                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        resolver2Attempts++;
                        return Task.FromResult(BuildNsResponse("ns1.example.net"));
                    }

                    if (address.Equals(authorityAddress))
                    {
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected resolver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(new[] { authorityAddress }));

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(1, resolver1Attempts);
            Assert.Equal(1, resolver2Attempts);
        }

        /// <summary>
        /// Verifies resolver fallback continues after transport failure and succeeds with the next resolver.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenFirstResolverThrowsSocketException_UsesNextResolverAndSucceeds()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";
            IPAddress authorityAddress = IPAddress.Parse("203.0.113.10");

            int resolver2Attempts = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.1")))
                    {
                        throw new SocketException((int)SocketError.HostUnreachable);
                    }

                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        resolver2Attempts++;
                        return Task.FromResult(BuildNsResponse("ns1.example.net"));
                    }

                    if (address.Equals(authorityAddress))
                    {
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected resolver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(new[] { authorityAddress }));

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(1, resolver2Attempts);
        }

        /// <summary>
        /// Verifies authoritative timeout before quorum is treated as failed observation and quorum can still succeed.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenAuthoritativeTimeoutOccursBeforeQuorum_ContinuesAndSucceeds()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 0.6);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            IPAddress authorityA = IPAddress.Parse("2001:db8::10");
            IPAddress authorityB = IPAddress.Parse("203.0.113.20");
            IPAddress authorityC = IPAddress.Parse("203.0.113.30");

            int authorityAQueries = 0;
            int authorityBQueries = 0;
            int authorityCQueries = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.2")) && LooksLikeNsQuery(request))
                    {
                        return Task.FromResult(BuildNsResponse("ns-a.example.net", "ns-b.example.net", "ns-c.example.net"));
                    }

                    if (address.Equals(authorityA))
                    {
                        authorityAQueries++;
                        throw new TimeoutException("simulated authority timeout");
                    }

                    if (address.Equals(authorityB))
                    {
                        authorityBQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    if (address.Equals(authorityC))
                    {
                        authorityCQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected nameserver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(hostName switch
                {
                    "ns-a.example.net" => new[] { authorityA },
                    "ns-b.example.net" => new[] { authorityB },
                    "ns-c.example.net" => new[] { authorityC },
                    _ => Array.Empty<IPAddress>(),
                }));

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(1, authorityAQueries);
            Assert.Equal(1, authorityBQueries);
            Assert.Equal(1, authorityCQueries);
        }

        /// <summary>
        /// Verifies polling stops once quorum is satisfied and remaining authorities are not required.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenQuorumSatisfiedEarly_ReturnsWithoutQueryingRemainingAuthorities()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 0.6);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            IPAddress authorityA = IPAddress.Parse("203.0.113.40");
            IPAddress authorityB = IPAddress.Parse("203.0.113.41");
            IPAddress authorityC = IPAddress.Parse("203.0.113.42");

            int authorityAQueries = 0;
            int authorityBQueries = 0;
            int authorityCQueries = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.2")) && LooksLikeNsQuery(request))
                    {
                        return Task.FromResult(BuildNsResponse("ns-a.example.net", "ns-b.example.net", "ns-c.example.net"));
                    }

                    if (address.Equals(authorityA))
                    {
                        authorityAQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    if (address.Equals(authorityB))
                    {
                        authorityBQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    if (address.Equals(authorityC))
                    {
                        authorityCQueries++;
                        throw new TimeoutException("should not be queried after quorum");
                    }

                    throw new InvalidOperationException("unexpected nameserver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(hostName switch
                {
                    "ns-a.example.net" => new[] { authorityA },
                    "ns-b.example.net" => new[] { authorityB },
                    "ns-c.example.net" => new[] { authorityC },
                    _ => Array.Empty<IPAddress>(),
                }));

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(1, authorityAQueries);
            Assert.Equal(1, authorityBQueries);
            Assert.Equal(0, authorityCQueries);
        }

        /// <summary>
        /// Verifies mixed IPv6 and IPv4 authoritative failures are isolated and healthy authorities can satisfy quorum.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenIpv6AuthorityFails_Ipv4AuthoritiesCanStillSatisfyQuorum()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 0.6);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            IPAddress authorityV6 = IPAddress.Parse("2001:db8::99");
            IPAddress authorityV4A = IPAddress.Parse("198.51.100.10");
            IPAddress authorityV4B = IPAddress.Parse("198.51.100.11");

            int v6Queries = 0;
            int v4AQueries = 0;
            int v4BQueries = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.2")) && LooksLikeNsQuery(request))
                    {
                        return Task.FromResult(BuildNsResponse("ns-v6.example.net", "ns-v4a.example.net", "ns-v4b.example.net"));
                    }

                    if (address.Equals(authorityV6))
                    {
                        v6Queries++;
                        throw new SocketException((int)SocketError.NetworkUnreachable);
                    }

                    if (address.Equals(authorityV4A))
                    {
                        v4AQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    if (address.Equals(authorityV4B))
                    {
                        v4BQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected nameserver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(hostName switch
                {
                    "ns-v6.example.net" => new[] { authorityV6 },
                    "ns-v4a.example.net" => new[] { authorityV4A },
                    "ns-v4b.example.net" => new[] { authorityV4B },
                    _ => Array.Empty<IPAddress>(),
                }));

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(1, v6Queries);
            Assert.Equal(1, v4AQueries);
            Assert.Equal(1, v4BQueries);
        }

        /// <summary>
        /// Verifies genuine caller cancellation propagates and is not converted into fallback, timeout, or miss.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenCallerCancelsDuringDnsOperation_ThrowsOperationCanceledException()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 3,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            using CancellationTokenSource cts = new();
            int resolver2Attempts = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.1")))
                    {
                        cts.Cancel();
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        resolver2Attempts++;
                        if (LooksLikeNsQuery(request))
                        {
                            return Task.FromResult(BuildNsResponse("ns1.example.net"));
                        }

                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected resolver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(new[] { IPAddress.Parse("203.0.113.10") }));

            await Assert.ThrowsAsync<OperationCanceledException>(
                () => verifier.WaitForPropagationAsync(fqdn, expectedValue, options, cts.Token));

            Assert.Equal(0, resolver2Attempts);
        }

        /// <summary>
        /// Verifies internal timeout classification does not cancel the caller token and fallback continues.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenInternalTimeoutOccurs_CallerTokenRemainsActiveAndFallbackContinues()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";
            IPAddress authorityAddress = IPAddress.Parse("203.0.113.10");

            using CancellationTokenSource cts = new();

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.1")))
                    {
                        throw new TimeoutException("simulated internal timeout");
                    }

                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        return Task.FromResult(BuildNsResponse("ns1.example.net"));
                    }

                    if (address.Equals(authorityAddress))
                    {
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected resolver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(new[] { authorityAddress }),
                receiveDnsResponseAsync: null,
                dnsUdpReceiveTimeout: null);

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, cts.Token);

            Assert.False(cts.IsCancellationRequested);
        }

        /// <summary>
        /// Verifies authoritative nameserver hostname resolution isolates SocketException failures and uses later successful nameserver addresses.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenAuthoritativeHostResolutionFailsForOneNs_ContinuesWithRemainingAuthorities()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 0.6);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            IPAddress authorityA = IPAddress.Parse("203.0.113.51");
            IPAddress authorityB = IPAddress.Parse("203.0.113.52");
            int resolvedGoodHostCount = 0;
            int authorityAQueries = 0;
            int authorityBQueries = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        return Task.FromResult(BuildNsResponse("ns-bad.example.net", "ns-good-a.example.net", "ns-good-b.example.net"));
                    }

                    if (address.Equals(authorityA))
                    {
                        authorityAQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    if (address.Equals(authorityB))
                    {
                        authorityBQueries++;
                        return Task.FromResult(BuildTxtResponse(expectedValue));
                    }

                    throw new InvalidOperationException("unexpected nameserver");
                },
                resolveHostAddressesAsync: (hostName, _) =>
                {
                    if (string.Equals(hostName, "ns-bad.example.net", StringComparison.Ordinal))
                    {
                        throw new SocketException((int)SocketError.HostNotFound);
                    }

                    if (string.Equals(hostName, "ns-good-a.example.net", StringComparison.Ordinal))
                    {
                        resolvedGoodHostCount++;
                        return Task.FromResult(new[] { authorityA });
                    }

                    if (string.Equals(hostName, "ns-good-b.example.net", StringComparison.Ordinal))
                    {
                        resolvedGoodHostCount++;
                        return Task.FromResult(new[] { authorityB });
                    }

                    return Task.FromResult(Array.Empty<IPAddress>());
                },
                receiveDnsResponseAsync: null,
                dnsUdpReceiveTimeout: null);

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None);

            Assert.Equal(2, resolvedGoodHostCount);
            Assert.Equal(1, authorityAQueries);
            Assert.Equal(1, authorityBQueries);
        }

        /// <summary>
        /// Verifies verifier-owned receive cancellation is converted to TimeoutException classification and resolver fallback continues.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenReceiveCancelsFromInternalTimeout_ConvertsToTimeoutAndFallsBack()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";
            IPAddress authorityAddress = IPAddress.Parse("203.0.113.61");

            int internalTimeoutReceives = 0;
            int healthyResolverQueries = 0;
            using CancellationTokenSource callerCts = new();

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["127.0.0.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: null,
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(new[] { authorityAddress }),
                receiveDnsResponseAsync: (socket, buffer, remoteEndPoint, cancellationToken) =>
                {
                    if (remoteEndPoint is not IPEndPoint endpoint)
                    {
                        throw new XunitException("Expected IPEndPoint remote endpoint.");
                    }

                    if (endpoint.Address.Equals(IPAddress.Loopback))
                    {
                        internalTimeoutReceives++;
                        TaskCompletionSource<SocketReceiveFromResult> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
                        return new ValueTask<SocketReceiveFromResult>(completion.Task);
                    }

                    if (endpoint.Address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        healthyResolverQueries++;
                        return new ValueTask<SocketReceiveFromResult>(new SocketReceiveFromResult
                        {
                            RemoteEndPoint = endpoint,
                            ReceivedBytes = CopyPayload(buffer, BuildNsResponse("ns-good.example.net")),
                        });
                    }

                    if (endpoint.Address.Equals(authorityAddress))
                    {
                        return new ValueTask<SocketReceiveFromResult>(new SocketReceiveFromResult
                        {
                            RemoteEndPoint = endpoint,
                            ReceivedBytes = CopyPayload(buffer, BuildTxtResponse(expectedValue)),
                        });
                    }

                    throw new InvalidOperationException("unexpected receive endpoint");
                },
                dnsUdpReceiveTimeout: TimeSpan.Zero);

            await verifier.WaitForPropagationAsync(fqdn, expectedValue, options, callerCts.Token);

            Assert.Equal(1, internalTimeoutReceives);
            Assert.Equal(1, healthyResolverQueries);
            Assert.False(callerCts.IsCancellationRequested);
        }

        /// <summary>
        /// Verifies caller cancellation during default DNS UDP receive propagates OperationCanceledException and does not fall back to another resolver.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenCallerCancelsDuringDefaultDnsReceive_ThrowsOperationCanceledException()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 2,
                pollIntervalSeconds: 1,
                quorumRatio: 1.0);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";
            TaskCompletionSource receiveEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            int resolver2ReceiveCalls = 0;

            using CancellationTokenSource callerCts = new();

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["127.0.0.1", "192.0.2.2"],
                sendDnsUdpQueryAsync: null,
                resolveHostAddressesAsync: (hostName, cancellationToken) => Task.FromResult(new[] { IPAddress.Parse("203.0.113.80") }),
                receiveDnsResponseAsync: (socket, buffer, remoteEndPoint, cancellationToken) =>
                {
                    if (remoteEndPoint is not IPEndPoint endPoint)
                    {
                        throw new XunitException("Expected IPEndPoint remote endpoint.");
                    }

                    if (endPoint.Address.Equals(IPAddress.Loopback))
                    {
                        _ = receiveEntered.TrySetResult();
                        TaskCompletionSource<SocketReceiveFromResult> blockedReceive = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        cancellationToken.Register(() => blockedReceive.TrySetCanceled(cancellationToken));
                        return new ValueTask<SocketReceiveFromResult>(blockedReceive.Task);
                    }

                    if (endPoint.Address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        resolver2ReceiveCalls++;
                        return new ValueTask<SocketReceiveFromResult>(new SocketReceiveFromResult
                        {
                            RemoteEndPoint = endPoint,
                            ReceivedBytes = CopyPayload(buffer, BuildNsResponse("ns-unused.example.net")),
                        });
                    }

                    throw new InvalidOperationException("unexpected receive endpoint");
                },
                dnsUdpReceiveTimeout: TimeSpan.FromSeconds(30));

            Task verificationTask = verifier.WaitForPropagationAsync(fqdn, expectedValue, options, callerCts.Token);
            await receiveEntered.Task;
            callerCts.Cancel();

            OperationCanceledException exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => verificationTask);
            Assert.IsType<TaskCanceledException>(exception);
            Assert.IsNotType<TimeoutException>(exception);
            Assert.True(callerCts.IsCancellationRequested);
            Assert.Equal(0, resolver2ReceiveCalls);
        }

        /// <summary>
        /// Verifies when every authoritative probe fails the operation ends with overall propagation timeout, not transport exception leakage.
        /// </summary>
        [Fact]
        public async Task WaitForPropagationAsync_WhenAllAuthoritiesFail_ThrowsOverallPropagationTimeout()
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(
                pollTimeoutSeconds: 1,
                pollIntervalSeconds: 1,
                quorumRatio: 0.6);

            string fqdn = "_acme-challenge.backfiller01.usenet.ninja";
            string expectedValue = "expected-token";

            IPAddress authorityA = IPAddress.Parse("203.0.113.71");
            IPAddress authorityB = IPAddress.Parse("2001:db8::71");
            int authorityFailures = 0;

            AuthoritativeDnsTxtPropagationVerifier verifier = new(
                TimeProvider.System,
                NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance,
                resolveSystemNameServers: () => ["192.0.2.2"],
                sendDnsUdpQueryAsync: (address, request, cancellationToken) =>
                {
                    if (address.Equals(IPAddress.Parse("192.0.2.2")))
                    {
                        return Task.FromResult(BuildNsResponse("ns-a.example.net", "ns-b.example.net"));
                    }

                    if (address.Equals(authorityA))
                    {
                        authorityFailures++;
                        throw new SocketException((int)SocketError.NetworkUnreachable);
                    }

                    if (address.Equals(authorityB))
                    {
                        authorityFailures++;
                        throw new TimeoutException("simulated per-authority timeout");
                    }

                    throw new InvalidOperationException("unexpected nameserver");
                },
                resolveHostAddressesAsync: (hostName, _) => Task.FromResult(hostName switch
                {
                    "ns-a.example.net" => new[] { authorityA },
                    "ns-b.example.net" => new[] { authorityB },
                    _ => Array.Empty<IPAddress>(),
                }),
                receiveDnsResponseAsync: null,
                dnsUdpReceiveTimeout: null);

            TimeoutException exception = await Assert.ThrowsAsync<TimeoutException>(
                () => verifier.WaitForPropagationAsync(fqdn, expectedValue, options, CancellationToken.None));

            Assert.Contains("Authoritative DNS TXT propagation timeout exceeded", exception.Message, StringComparison.Ordinal);
            Assert.True(authorityFailures >= 2);
        }

        /// <summary>
        /// Builds one deterministic ACME options snapshot for authoritative DNS propagation tests.
        /// </summary>
        private static BackFillerLetsEncryptRuntimeOptions CreateLetsEncryptOptions(int pollTimeoutSeconds, int pollIntervalSeconds, double quorumRatio)
        {
            return new BackFillerLetsEncryptRuntimeOptions(
                CanonicalCertificateSubjectName: "backfiller01.usenet.ninja",
                AcmeAccountEmail: "security@example.com",
                AcmeAccountKeyPemPath: "account.key",
                CertificatePfxPath: "certificate.pfx",
                CertificatePrivateKeyPemPath: "certificate.key",
                PfxExportPassword: "UnitTest-PfxPassword-123!",
                RenewBeforeExpiryDays: 7,
                RenewalCheckIntervalHours: 6,
                RenewalJitterRatio: 0.1,
                UseStagingDirectory: true,
                AcmeTransientRetryMaxAttempts: 5,
                DnsPropagationDelaySeconds: 0,
                DnsTxtPollIntervalSeconds: pollIntervalSeconds,
                DnsTxtPollTimeoutSeconds: pollTimeoutSeconds,
                DnsAuthoritativeNsCacheMinutes: 1,
                DnsAuthoritativeQuorumRatio: quorumRatio,
                CloudFlareApiToken: "token",
                CloudFlareZoneId: "zone");
        }

        /// <summary>
        /// Detects whether one request payload targets NS records.
        /// </summary>
        private static bool LooksLikeNsQuery(byte[] request)
        {
            return request.Length >= 4
                && request[^4] == 0
                && request[^3] == 2
                && request[^2] == 0
                && request[^1] == 1;
        }

        /// <summary>
        /// Builds one minimal DNS response containing NS answers for the provided nameserver host names.
        /// </summary>
        private static byte[] BuildNsResponse(params string[] nameserverHostNames)
        {
            return BuildResponse(
                answerTypeCode: 2,
                answerPayloads: nameserverHostNames.Select(EncodeDnsName).ToArray());
        }

        /// <summary>
        /// Builds one minimal DNS response containing exactly one TXT answer.
        /// </summary>
        private static byte[] BuildTxtResponse(string txtValue)
        {
            byte[] txtBytes = System.Text.Encoding.ASCII.GetBytes(txtValue);
            byte[] payload = new byte[txtBytes.Length + 1];
            payload[0] = (byte)txtBytes.Length;
            Buffer.BlockCopy(txtBytes, 0, payload, 1, txtBytes.Length);

            return BuildResponse(answerTypeCode: 16, answerPayloads: [payload]);
        }

        /// <summary>
        /// Builds one minimal DNS response with the provided answer payloads.
        /// </summary>
        private static byte[] BuildResponse(ushort answerTypeCode, IReadOnlyList<byte[]> answerPayloads)
        {
            using MemoryStream stream = new();
            WriteUInt16(stream, 0xCAFE);
            WriteUInt16(stream, 0x8180);
            WriteUInt16(stream, 1);
            WriteUInt16(stream, (ushort)answerPayloads.Count);
            WriteUInt16(stream, 0);
            WriteUInt16(stream, 0);

            stream.Write(EncodeDnsName("example.net"));
            WriteUInt16(stream, 16);
            WriteUInt16(stream, 1);

            for (int index = 0; index < answerPayloads.Count; index++)
            {
                byte[] payload = answerPayloads[index];
                stream.WriteByte(0xC0);
                stream.WriteByte(0x0C);
                WriteUInt16(stream, answerTypeCode);
                WriteUInt16(stream, 1);
                WriteUInt32(stream, 30);
                WriteUInt16(stream, (ushort)payload.Length);
                stream.Write(payload, 0, payload.Length);
            }

            return stream.ToArray();
        }

        /// <summary>
        /// Encodes one DNS name as wire-format labels.
        /// </summary>
        private static byte[] EncodeDnsName(string value)
        {
            string[] labels = value.Trim().TrimEnd('.').Split('.', StringSplitOptions.RemoveEmptyEntries);
            using MemoryStream stream = new();
            for (int index = 0; index < labels.Length; index++)
            {
                byte[] labelBytes = System.Text.Encoding.ASCII.GetBytes(labels[index]);
                stream.WriteByte((byte)labelBytes.Length);
                stream.Write(labelBytes, 0, labelBytes.Length);
            }

            stream.WriteByte(0);
            return stream.ToArray();
        }

        /// <summary>
        /// Writes one 16-bit unsigned integer using network byte order.
        /// </summary>
        private static void WriteUInt16(Stream stream, ushort value)
        {
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)(value & 0xFF));
        }

        /// <summary>
        /// Writes one 32-bit unsigned integer using network byte order.
        /// </summary>
        private static void WriteUInt32(Stream stream, uint value)
        {
            stream.WriteByte((byte)(value >> 24));
            stream.WriteByte((byte)((value >> 16) & 0xFF));
            stream.WriteByte((byte)((value >> 8) & 0xFF));
            stream.WriteByte((byte)(value & 0xFF));
        }

        /// <summary>
        /// Copies one wire-format DNS payload into the receive buffer and returns the received-byte count.
        /// </summary>
        private static int CopyPayload(Memory<byte> destination, byte[] payload)
        {
            payload.CopyTo(destination);
            return payload.Length;
        }
    }
}
