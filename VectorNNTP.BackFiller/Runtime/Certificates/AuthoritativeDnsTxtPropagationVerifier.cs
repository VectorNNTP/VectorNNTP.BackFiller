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
        /// Stores time provider used by authoritative dns txt propagation verifier.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Supplies the logger used by authoritative dns txt propagation verifier.
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

        /// <inheritdoc/>
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
                        _logger.LogInformation(
                            "Authoritative DNS TXT propagation verified for {Fqdn}; Matched={Matched}; Required={Required}; TotalNameservers={TotalNameservers}",
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
        /// Handles resolve authoritative name server addresses async for authoritative dns txt propagation verifier.
        /// </summary>
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
        /// Handles query ns record names from system resolvers async for authoritative dns txt propagation verifier.
        /// </summary>
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
                    IReadOnlyList<string> names = DnsWireMessageParser.ParseNsRecordNames(response);
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
        /// Handles resolve system name servers for authoritative dns txt propagation verifier.
        /// </summary>
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
        /// Handles query txt contains value async for authoritative dns txt propagation verifier.
        /// </summary>
        private static async Task<bool> QueryTxtContainsValueAsync(IPAddress nameServer, string fqdn, string expectedTxtValue, CancellationToken cancellationToken)
        {
            byte[] request = DnsWireMessageBuilder.BuildQuery(fqdn, DnsRecordTypeCode.Txt);
            byte[] response = await SendDnsUdpQueryAsync(nameServer, request, cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> txtValues = DnsWireMessageParser.ParseTxtValues(response);
            return txtValues.Any(value => string.Equals(value, expectedTxtValue, StringComparison.Ordinal));
        }

        /// <summary>
        /// Handles send dns udp query async for authoritative dns txt propagation verifier.
        /// </summary>
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
        /// Handles normalize dns name for authoritative dns txt propagation verifier.
        /// </summary>
        private static string NormalizeDnsName(string value)
        {
            return value.Trim().TrimEnd('.').ToLowerInvariant();
        }

        /// <summary>
        /// Defines dns record type code and its authoritative dns txt propagation verifier contract.
        /// </summary>
        private enum DnsRecordTypeCode : ushort
        {
            Ns = 2,
            Txt = 16,
        }

        /// <summary>
        /// Defines dns wire message builder and its authoritative dns txt propagation verifier contract.
        /// </summary>
        private static class DnsWireMessageBuilder
        {
            /// <summary>
            /// Handles build query for authoritative dns txt propagation verifier.
            /// </summary>
            /// <param name="fqdn">The fqdn value.</param>
            /// <param name="type">The type value.</param>
            /// <returns>The operation result.</returns>
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
            /// Handles encode dns name for authoritative dns txt propagation verifier.
            /// </summary>
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
            /// Handles write uint16 for authoritative dns txt propagation verifier.
            /// </summary>
            private static void WriteUInt16(byte[] buffer, int offset, ushort value)
            {
                buffer[offset] = (byte)(value >> 8);
                buffer[offset + 1] = (byte)(value & 0xFF);
            }
        }

        /// <summary>
        /// Defines dns wire message parser and its authoritative dns txt propagation verifier contract.
        /// </summary>
        private static class DnsWireMessageParser
        {
            /// <summary>
            /// Handles parse ns record names for authoritative dns txt propagation verifier.
            /// </summary>
            /// <param name="message">The message value.</param>
            /// <returns>The operation result.</returns>
            internal static IReadOnlyList<string> ParseNsRecordNames(byte[] message)
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
            /// Handles parse txt values for authoritative dns txt propagation verifier.
            /// </summary>
            /// <param name="message">The message value.</param>
            /// <returns>The operation result.</returns>
            internal static IReadOnlyList<string> ParseTxtValues(byte[] message)
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
            /// Handles static for authoritative dns txt propagation verifier.
            /// </summary>
            /// <param name="message">The message value.</param>
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
            /// Handles skip name for authoritative dns txt propagation verifier.
            /// </summary>
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
            /// Handles read name for authoritative dns txt propagation verifier.
            /// </summary>
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
            /// Handles read uint16 for authoritative dns txt propagation verifier.
            /// </summary>
            private static ushort ReadUInt16(byte[] message, int offset)
            {
                return (ushort)((message[offset] << 8) | message[offset + 1]);
            }

            /// <summary>
            /// Handles read uint16 for authoritative dns txt propagation verifier.
            /// </summary>
            private static ushort ReadUInt16(byte[] message, ref int offset)
            {
                ushort value = ReadUInt16(message, offset);
                offset += 2;
                return value;
            }

            /// <summary>
            /// Handles read uint32 for authoritative dns txt propagation verifier.
            /// </summary>
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
