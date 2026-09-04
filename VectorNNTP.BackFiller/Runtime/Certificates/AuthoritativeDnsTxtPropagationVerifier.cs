// <copyright file="AuthoritativeDnsTxtPropagationVerifier.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Verifies authoritative DNS TXT visibility for ACME DNS-01 challenge propagation.

using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Verifies authoritative DNS TXT propagation by querying discovered nameservers directly.
    /// </summary>
    /// <remarks>
    /// This verifier is used only for ACME DNS-01 challenge readiness. It resolves the authoritative nameservers for
    /// the challenge zone, queries them directly, and waits until the configured quorum sees the expected TXT value.
    /// </remarks>
    internal sealed partial class AuthoritativeDnsTxtPropagationVerifier : IAuthoritativeDnsTxtPropagationVerifier
    {
        /// <summary>
        /// Clock source used for propagation deadlines and poll scheduling.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Logger used for successful authoritative-propagation diagnostics.
        /// </summary>
        private readonly ILogger<AuthoritativeDnsTxtPropagationVerifier> _logger;

        /// <summary>
        /// Initializes the authoritative TXT propagation verifier.
        /// </summary>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="logger">Logger for propagation diagnostics.</param>
        public AuthoritativeDnsTxtPropagationVerifier(
            TimeProvider timeProvider,
            ILogger<AuthoritativeDnsTxtPropagationVerifier> logger)
        {
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(logger);

            _timeProvider = timeProvider;
            _logger = logger;
        }

        /// <summary>
        /// Waits until enough authoritative nameservers return the expected TXT value for the ACME challenge host name.
        /// </summary>
        /// <remarks>
        /// The verifier first resolves authoritative nameserver addresses, optionally waits the configured initial delay,
        /// and then polls each discovered server directly until the configured quorum is satisfied or the timeout
        /// expires. Recursive-resolver caches are bypassed after nameserver discovery.
        /// </remarks>
        /// <param name="fqdn">Fully qualified ACME TXT host name to verify.</param>
        /// <param name="expectedTxtValue">TXT value that ACME validation must observe.</param>
        /// <param name="options">Validated ACME runtime options that define propagation delay, poll cadence, timeout, and quorum.</param>
        /// <param name="cancellationToken">Cancellation token that aborts propagation waiting.</param>
        /// <returns>A task that completes when authoritative propagation criteria are met.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no authoritative nameserver addresses can be resolved for the challenge name.</exception>
        /// <exception cref="TimeoutException">Thrown when the required authoritative quorum does not observe the expected TXT value before the timeout expires.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested before propagation succeeds.</exception>
        public async Task WaitForPropagationAsync(
            string fqdn,
            string expectedTxtValue,
            BackFillerLetsEncryptRuntimeOptions options,
            CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedTxtValue);
            ArgumentNullException.ThrowIfNull(options);

            cancellationToken.ThrowIfCancellationRequested();

            string normalizedFqdn = NormalizeDnsName(fqdn);
            IReadOnlyList<IPAddress> authoritativeNameServers = await ResolveAuthoritativeNameServerAddressesAsync(normalizedFqdn, cancellationToken).ConfigureAwait(false);
            if (authoritativeNameServers.Count == 0)
            {
                throw new InvalidOperationException($"Unable to resolve authoritative nameservers for '{normalizedFqdn}'.");
            }

            TimeSpan timeout = TimeSpan.FromSeconds(options.DnsTxtPollTimeoutSeconds);
            TimeSpan interval = TimeSpan.FromSeconds(options.DnsTxtPollIntervalSeconds);
            DateTimeOffset deadline = _timeProvider.GetUtcNow().Add(timeout);

            TimeSpan initialDelay = TimeSpan.FromSeconds(options.DnsPropagationDelaySeconds);
            if (initialDelay > TimeSpan.Zero)
            {
                await Task.Delay(initialDelay, cancellationToken).ConfigureAwait(false);
            }

            while (_timeProvider.GetUtcNow() <= deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int successCount = 0;
                for (int index = 0; index < authoritativeNameServers.Count; index++)
                {
                    IPAddress nameServer = authoritativeNameServers[index];
                    bool matched = await QueryTxtContainsValueAsync(nameServer, normalizedFqdn, expectedTxtValue, cancellationToken).ConfigureAwait(false);
                    if (matched)
                    {
                        successCount++;
                    }
                }

                double quorum = authoritativeNameServers.Count * options.DnsAuthoritativeQuorumRatio;
                int requiredSuccesses = Math.Max(1, (int)Math.Ceiling(quorum));
                if (successCount >= requiredSuccesses)
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        LogAuthoritativeDnsTxtPropagationVerified(
                            _logger,
                            normalizedFqdn,
                            successCount,
                            requiredSuccesses,
                            authoritativeNameServers.Count);
                    }
                    return;
                }

                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }

            throw new TimeoutException($"Authoritative DNS TXT propagation timeout exceeded for '{normalizedFqdn}'.");
        }

        /// <summary>
        /// Emits the authoritative DNS TXT propagation success log event once the required authoritative quorum observes the expected TXT value.
        /// </summary>
        /// <param name="logger">Logger receiving the propagation-success event.</param>
        /// <param name="fqdn">Fully qualified ACME TXT host name whose propagation was verified.</param>
        /// <param name="matched">Number of authoritative nameservers that observed the expected TXT value.</param>
        /// <param name="required">Minimum authoritative quorum required for propagation to be considered complete.</param>
        /// <param name="totalNameservers">Total number of authoritative nameservers queried for the zone.</param>
        [LoggerMessage(
            EventId = 2804,
            Level = LogLevel.Information,
            Message = "Authoritative DNS TXT propagation verified for {Fqdn}; Matched={Matched}; Required={Required}; TotalNameservers={TotalNameservers}")]
        private static partial void LogAuthoritativeDnsTxtPropagationVerified(
            ILogger logger,
            string fqdn,
            int matched,
            int required,
            int totalNameservers);

        /// <summary>
        /// Resolves IP addresses for the authoritative nameservers responsible for the candidate DNS zone.
        /// </summary>
        /// <remarks>
        /// The search walks from the full challenge name toward parent labels until it finds NS records, then resolves
        /// those nameserver host names to IPv4/IPv6 addresses.
        /// </remarks>
        /// <param name="fqdn">Normalized challenge host name whose authoritative zone should be discovered.</param>
        /// <param name="cancellationToken">Cancellation token observed while querying recursive resolvers and resolving nameserver host names.</param>
        /// <returns>The discovered authoritative nameserver addresses, or an empty list when discovery fails.</returns>
        private static async Task<IReadOnlyList<IPAddress>> ResolveAuthoritativeNameServerAddressesAsync(string fqdn, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

            string[] labels = fqdn.Trim('.').Split('.', StringSplitOptions.RemoveEmptyEntries);
            for (int start = 0; start < labels.Length; start++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string candidate = string.Join('.', labels.Skip(start));
                IReadOnlyList<string> nsNames = await QueryNsRecordNamesFromSystemResolversAsync(candidate, cancellationToken).ConfigureAwait(false);
                if (nsNames.Count == 0)
                {
                    continue;
                }

                HashSet<IPAddress> addresses = [];
                for (int i = 0; i < nsNames.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string nsName = nsNames[i];
                    IPAddress[] hostAddresses = await Dns.GetHostAddressesAsync(nsName, cancellationToken).ConfigureAwait(false);
                    for (int j = 0; j < hostAddresses.Length; j++)
                    {
                        if (hostAddresses[j].AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                        {
                            _ = addresses.Add(hostAddresses[j]);
                        }
                    }
                }

                if (addresses.Count > 0)
                {
                    return [.. addresses];
                }
            }

            return [];
        }

        /// <summary>
        /// Queries system-configured recursive resolvers for NS records for one candidate zone name.
        /// </summary>
        /// <remarks>
        /// Resolver failures are treated as probe misses so the caller can continue trying additional configured system
        /// nameservers.
        /// </remarks>
        /// <param name="zoneName">Candidate zone name whose NS records should be discovered.</param>
        /// <param name="cancellationToken">Cancellation token observed while querying the recursive resolvers.</param>
        /// <returns>Normalized nameserver host names returned by the first resolver that produces any NS answers.</returns>
        private static async Task<IReadOnlyList<string>> QueryNsRecordNamesFromSystemResolversAsync(string zoneName, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneName);

            byte[] request = DnsWireMessageBuilder.BuildQuery(zoneName, DnsRecordTypeCode.Ns);

            string[] systemNameServers = ResolveSystemNameServers();
            for (int index = 0; index < systemNameServers.Length; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!IPAddress.TryParse(systemNameServers[index], out IPAddress? nameServerAddress))
                {
                    continue;
                }

                try
                {
                    byte[] response = await SendDnsUdpQueryAsync(nameServerAddress, request, cancellationToken).ConfigureAwait(false);
                    List<string> names = DnsWireMessageParser.ParseNsRecordNames(response);
                    if (names.Count > 0)
                    {
                        return names;
                    }
                }
                catch (SocketException)
                {
                }
                catch (TimeoutException)
                {
                }
            }

            return [];
        }

        /// <summary>
        /// Returns the recursive resolvers used to discover authoritative NS records.
        /// </summary>
        /// <remarks>
        /// On Linux and macOS the resolver list is read from <c>/etc/resolv.conf</c>. When no usable entries are found,
        /// the implementation falls back to a small public-resolver set so authoritative discovery can still proceed.
        /// </remarks>
        /// <returns>Distinct resolver IP-address strings in probe order.</returns>
        private static string[] ResolveSystemNameServers()
        {
            List<string> servers = [];

            string resolvConfPath = "/etc/resolv.conf";
            if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            {
                if (File.Exists(resolvConfPath))
                {
                    string[] lines = File.ReadAllLines(resolvConfPath);
                    foreach (string line in lines)
                    {
                        string trimmed = line.Trim();
                        if (!trimmed.StartsWith("nameserver ", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        string[] parts = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length >= 2)
                        {
                            servers.Add(parts[1]);
                        }
                    }
                }
            }

            if (servers.Count == 0)
            {
                servers.Add("1.1.1.1");
                servers.Add("8.8.8.8");
                servers.Add("2606:4700:4700::1111");
                servers.Add("2001:4860:4860::8888");
            }

            return [.. servers.Distinct(StringComparer.OrdinalIgnoreCase)];
        }

        /// <summary>
        /// Queries one authoritative nameserver and checks whether any TXT answer exactly matches the expected value.
        /// </summary>
        /// <param name="nameServer">Authoritative nameserver address to query.</param>
        /// <param name="fqdn">Normalized challenge host name to request.</param>
        /// <param name="expectedTxtValue">TXT payload expected in the nameserver response.</param>
        /// <param name="cancellationToken">Cancellation token observed while sending and receiving the DNS query.</param>
        /// <returns><see langword="true"/> when the nameserver response contains the expected TXT value.</returns>
        private static async Task<bool> QueryTxtContainsValueAsync(IPAddress nameServer, string fqdn, string expectedTxtValue, CancellationToken cancellationToken)
        {
            byte[] request = DnsWireMessageBuilder.BuildQuery(fqdn, DnsRecordTypeCode.Txt);
            byte[] response = await SendDnsUdpQueryAsync(nameServer, request, cancellationToken).ConfigureAwait(false);
            List<string> txtValues = DnsWireMessageParser.ParseTxtValues(response);
            return txtValues.Any(value => string.Equals(value, expectedTxtValue, StringComparison.Ordinal));
        }

        /// <summary>
        /// Sends one DNS query over UDP and returns the received response payload.
        /// </summary>
        /// <remarks>
        /// Each query uses a fresh datagram socket and applies an internal five-second receive timeout in addition to the
        /// caller's cancellation token.
        /// </remarks>
        /// <param name="nameServer">Resolver or authoritative nameserver address to query.</param>
        /// <param name="request">Wire-format DNS query bytes.</param>
        /// <param name="cancellationToken">Cancellation token observed while sending and receiving.</param>
        /// <returns>The received DNS response bytes.</returns>
        private static async Task<byte[]> SendDnsUdpQueryAsync(IPAddress nameServer, byte[] request, CancellationToken cancellationToken)
        {
            using Socket socket = new(nameServer.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            EndPoint remoteEndPoint = new IPEndPoint(nameServer, 53);

            _ = await socket.SendToAsync(request, SocketFlags.None, remoteEndPoint, cancellationToken).ConfigureAwait(false);

            byte[] buffer = new byte[4096];
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));

            SocketReceiveFromResult result = await socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteEndPoint, timeout.Token).ConfigureAwait(false);
            return buffer[..result.ReceivedBytes];
        }

        /// <summary>
        /// Normalizes a DNS name for comparisons and wire-format generation.
        /// </summary>
        /// <param name="value">DNS name to normalize.</param>
        /// <returns>Lower-cased DNS name without surrounding whitespace or a trailing root dot.</returns>
        private static string NormalizeDnsName(string value)
        {
            return value.Trim().TrimEnd('.').ToLowerInvariant();
        }

        /// <summary>
        /// DNS record type codes used by the minimal wire-format query builder and parser.
        /// </summary>
        private enum DnsRecordTypeCode : ushort
        {
            Ns = 2,
            Txt = 16,
        }

        /// <summary>
        /// Builds the minimal DNS wire-format queries needed for authoritative NS and TXT lookups.
        /// </summary>
        private static class DnsWireMessageBuilder
        {
            /// <summary>
            /// Builds a standard-recursion-desired DNS query for the supplied name and record type.
            /// </summary>
            /// <param name="fqdn">DNS name to encode into the question section.</param>
            /// <param name="type">Record type to request.</param>
            /// <returns>Wire-format DNS query bytes.</returns>
            internal static byte[] BuildQuery(string fqdn, DnsRecordTypeCode type)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

                ushort transactionId = (ushort)Random.Shared.Next(0, ushort.MaxValue + 1);
                byte[] header = new byte[12];

                WriteUInt16(header, 0, transactionId);
                WriteUInt16(header, 2, 0x0100);
                WriteUInt16(header, 4, 1);
                WriteUInt16(header, 6, 0);
                WriteUInt16(header, 8, 0);
                WriteUInt16(header, 10, 0);

                byte[] questionName = EncodeDnsName(fqdn);
                byte[] questionTail = new byte[4];
                WriteUInt16(questionTail, 0, (ushort)type);
                WriteUInt16(questionTail, 2, 1);

                byte[] message = new byte[header.Length + questionName.Length + questionTail.Length];
                Buffer.BlockCopy(header, 0, message, 0, header.Length);
                Buffer.BlockCopy(questionName, 0, message, header.Length, questionName.Length);
                Buffer.BlockCopy(questionTail, 0, message, header.Length + questionName.Length, questionTail.Length);
                return message;
            }

            /// <summary>
            /// Encodes a DNS name into length-prefixed wire-format labels terminated by a root label.
            /// </summary>
            /// <param name="fqdn">DNS name to encode.</param>
            /// <returns>Wire-format label bytes for <paramref name="fqdn"/>.</returns>
            private static byte[] EncodeDnsName(string fqdn)
            {
                string[] labels = fqdn.Trim().TrimEnd('.').Split('.', StringSplitOptions.RemoveEmptyEntries);
                using MemoryStream ms = new();
                for (int i = 0; i < labels.Length; i++)
                {
                    byte[] labelBytes = Encoding.ASCII.GetBytes(labels[i]);
                    ms.WriteByte((byte)labelBytes.Length);
                    ms.Write(labelBytes, 0, labelBytes.Length);
                }

                ms.WriteByte(0);
                return ms.ToArray();
            }

            /// <summary>
            /// Writes one unsigned 16-bit integer in network byte order.
            /// </summary>
            /// <param name="buffer">Buffer receiving the encoded value.</param>
            /// <param name="offset">Offset at which the value should be written.</param>
            /// <param name="value">Unsigned 16-bit value to encode.</param>
            private static void WriteUInt16(byte[] buffer, int offset, ushort value)
            {
                buffer[offset] = (byte)(value >> 8);
                buffer[offset + 1] = (byte)(value & 0xFF);
            }
        }

        /// <summary>
        /// Parses the DNS wire-format responses needed for authoritative NS and TXT verification.
        /// </summary>
        private static class DnsWireMessageParser
        {
            /// <summary>
            /// Parses NS record names from the answer and authority sections of a DNS response.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <returns>Normalized NS host names discovered in the response.</returns>
            internal static List<string> ParseNsRecordNames(byte[] message)
            {
                ArgumentNullException.ThrowIfNull(message);
                (int questionCount, int answerCount, int authorityCount, _) = ReadHeaderCounts(message);
                int offset = 12;

                for (int i = 0; i < questionCount; i++)
                {
                    offset = SkipName(message, offset);
                    offset += 4;
                }

                List<string> result = [];
                int totalRr = answerCount + authorityCount;
                for (int i = 0; i < totalRr; i++)
                {
                    offset = SkipName(message, offset);
                    ushort type = ReadUInt16(message, ref offset);
                    _ = ReadUInt16(message, ref offset);
                    _ = ReadUInt32(message, ref offset);
                    ushort rdLength = ReadUInt16(message, ref offset);

                    int rdataStart = offset;
                    if (type == (ushort)DnsRecordTypeCode.Ns)
                    {
                        int nsOffset = rdataStart;
                        string nsName = ReadName(message, ref nsOffset);
                        if (!string.IsNullOrWhiteSpace(nsName))
                        {
                            result.Add(NormalizeDnsName(nsName));
                        }
                    }

                    offset = rdataStart + rdLength;
                }

                return result;
            }

            /// <summary>
            /// Parses TXT values from the answer section of a DNS response.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <returns>TXT payload strings reconstructed from the response.</returns>
            internal static List<string> ParseTxtValues(byte[] message)
            {
                ArgumentNullException.ThrowIfNull(message);
                (int questionCount, int answerCount, _, _) = ReadHeaderCounts(message);
                int offset = 12;

                for (int i = 0; i < questionCount; i++)
                {
                    offset = SkipName(message, offset);
                    offset += 4;
                }

                List<string> values = [];
                for (int i = 0; i < answerCount; i++)
                {
                    offset = SkipName(message, offset);
                    ushort type = ReadUInt16(message, ref offset);
                    _ = ReadUInt16(message, ref offset);
                    _ = ReadUInt32(message, ref offset);
                    ushort rdLength = ReadUInt16(message, ref offset);

                    int rdataStart = offset;
                    if (type == (ushort)DnsRecordTypeCode.Txt)
                    {
                        int end = rdataStart + rdLength;
                        StringBuilder sb = new();
                        int readOffset = rdataStart;
                        while (readOffset < end)
                        {
                            byte len = message[readOffset++];
                            if (len == 0 || readOffset + len > end)
                            {
                                break;
                            }

                            _ = sb.Append(Encoding.ASCII.GetString(message, readOffset, len));
                            readOffset += len;
                        }

                        values.Add(sb.ToString());
                    }

                    offset = rdataStart + rdLength;
                }

                return values;
            }

            /// <summary>
            /// Reads the DNS header section counts from a response message.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <returns>The question, answer, authority, and additional-record counts from the DNS header.</returns>
            private static (int QuestionCount, int AnswerCount, int AuthorityCount, int AdditionalCount) ReadHeaderCounts(byte[] message)
            {
                if (message.Length < 12)
                {
                    throw new InvalidOperationException("Invalid DNS response header.");
                }

                int questionCount = ReadUInt16(message, 4);
                int answerCount = ReadUInt16(message, 6);
                int authorityCount = ReadUInt16(message, 8);
                int additionalCount = ReadUInt16(message, 10);
                return (questionCount, answerCount, authorityCount, additionalCount);
            }

            /// <summary>
            /// Advances past one encoded DNS name, including compression pointers.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <param name="offset">Current offset of the encoded name.</param>
            /// <returns>The offset immediately after the encoded name.</returns>
            private static int SkipName(byte[] message, int offset)
            {
                while (offset < message.Length)
                {
                    byte len = message[offset++];
                    if (len == 0)
                    {
                        return offset;
                    }

                    if ((len & 0xC0) == 0xC0)
                    {
                        return offset + 1;
                    }

                    offset += len;
                }

                throw new InvalidOperationException("Invalid DNS name encoding.");
            }

            /// <summary>
            /// Reads one DNS name, expanding compression pointers when present.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <param name="offset">Offset of the encoded name. On success the offset advances past the consumed bytes.</param>
            /// <returns>The decoded DNS name.</returns>
            private static string ReadName(byte[] message, ref int offset)
            {
                List<string> labels = [];
                int current = offset;
                bool jumped = false;

                while (current < message.Length)
                {
                    byte len = message[current++];
                    if (len == 0)
                    {
                        if (!jumped)
                        {
                            offset = current;
                        }

                        return string.Join('.', labels);
                    }

                    if ((len & 0xC0) == 0xC0)
                    {
                        if (current >= message.Length)
                        {
                            throw new InvalidOperationException("Invalid DNS compression pointer.");
                        }

                        int pointer = ((len & 0x3F) << 8) | message[current++];
                        if (!jumped)
                        {
                            offset = current;
                        }

                        current = pointer;
                        jumped = true;
                        continue;
                    }

                    if (current + len > message.Length)
                    {
                        throw new InvalidOperationException("Invalid DNS label length.");
                    }

                    labels.Add(Encoding.ASCII.GetString(message, current, len));
                    current += len;
                }

                throw new InvalidOperationException("Invalid DNS name encoding.");
            }

            /// <summary>
            /// Reads one unsigned 16-bit integer from a fixed response offset.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <param name="offset">Offset of the value to decode.</param>
            /// <returns>The decoded unsigned 16-bit integer.</returns>
            private static ushort ReadUInt16(byte[] message, int offset)
            {
                return (ushort)((message[offset] << 8) | message[offset + 1]);
            }

            /// <summary>
            /// Reads one unsigned 16-bit integer and advances the supplied response offset.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <param name="offset">Offset of the value to decode. The method advances it past the decoded bytes.</param>
            /// <returns>The decoded unsigned 16-bit integer.</returns>
            private static ushort ReadUInt16(byte[] message, ref int offset)
            {
                ushort value = ReadUInt16(message, offset);
                offset += 2;
                return value;
            }

            /// <summary>
            /// Reads one unsigned 32-bit integer and advances the supplied response offset.
            /// </summary>
            /// <param name="message">Wire-format DNS response bytes.</param>
            /// <param name="offset">Offset of the value to decode. The method advances it past the decoded bytes.</param>
            /// <returns>The decoded unsigned 32-bit integer.</returns>
            private static uint ReadUInt32(byte[] message, ref int offset)
            {
                uint value = (uint)((message[offset] << 24) |
                                    (message[offset + 1] << 16) |
                                    (message[offset + 2] << 8) |
                                    message[offset + 3]);
                offset += 4;
                return value;
            }
        }
    }
}
