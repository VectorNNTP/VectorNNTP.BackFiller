// <copyright file="BindAddressDnsAddressDeriver.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Derives canonical DNS-advertised addresses from BindAddress configuration semantics.

using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Derives canonical DNS address sets from BackFiller bind-address configuration.
    /// </summary>
    /// <remarks>
    /// <para>Explicit bind-address configuration yields an exact parsed IP-address set.</para>
    /// <para>Wildcard configuration markers (<c>*</c>, <c>Any</c>, <c>0.0.0.0</c>, and <c>::</c>) derive DNS addresses from eligible local interface addresses.</para>
    /// </remarks>
    internal static class BindAddressDnsAddressDeriver
    {
        /// <summary>
        /// Derives canonical DNS addresses from configured bind-address values.
        /// </summary>
        /// <param name="bindAddresses">Configured bind-address values.</param>
        /// <param name="interfaceAddressProvider">Optional provider that returns local interface addresses for wildcard derivation.</param>
        /// <returns>Canonical deduplicated DNS address set.</returns>
        /// <exception cref="InvalidOperationException">Thrown when configured bind-address values contain unparseable entries or wildcard interface enumeration fails.</exception>
        internal static IReadOnlyList<IPAddress> DeriveCanonicalDnsAddresses(
            string[]? bindAddresses,
            Func<IReadOnlyList<IPAddress>>? interfaceAddressProvider = null)
        {
            if (bindAddresses == null || bindAddresses.Length == 0)
            {
                return [];
            }

            HashSet<IPAddress> derivedAddresses = [];
            bool includeWildcardIpv4Addresses = false;
            bool includeWildcardIpv6Addresses = false;

            for (int index = 0; index < bindAddresses.Length; index++)
            {
                string configuredValue = bindAddresses[index];

                if (IsWildcardBindAddressToken(configuredValue))
                {
                    includeWildcardIpv4Addresses = true;
                    includeWildcardIpv6Addresses = true;
                    continue;
                }

                if (!IPAddress.TryParse(configuredValue, out IPAddress? parsedAddress))
                {
                    throw new InvalidOperationException($"BackFiller:BindAddress[{index}] is invalid and cannot be canonicalized into runtime options.");
                }

                if (IPAddress.Any.Equals(parsedAddress))
                {
                    includeWildcardIpv4Addresses = true;
                    continue;
                }

                if (IPAddress.IPv6Any.Equals(parsedAddress))
                {
                    includeWildcardIpv6Addresses = true;
                    continue;
                }

                if (IsEligibleDnsAddress(parsedAddress))
                {
                    _ = derivedAddresses.Add(parsedAddress);
                }
            }

            if (includeWildcardIpv4Addresses || includeWildcardIpv6Addresses)
            {
                IReadOnlyList<IPAddress> interfaceAddresses = interfaceAddressProvider is null
                    ? EnumerateInterfaceAddresses()
                    : interfaceAddressProvider();

                foreach (IPAddress interfaceAddress in interfaceAddresses)
                {
                    if (!IsEligibleDnsAddress(interfaceAddress))
                    {
                        continue;
                    }

                    if (includeWildcardIpv4Addresses && interfaceAddress.AddressFamily == AddressFamily.InterNetwork)
                    {
                        _ = derivedAddresses.Add(interfaceAddress);
                    }

                    if (includeWildcardIpv6Addresses && interfaceAddress.AddressFamily == AddressFamily.InterNetworkV6)
                    {
                        _ = derivedAddresses.Add(interfaceAddress);
                    }
                }
            }

            return CanonicalizeAddresses(derivedAddresses);
        }

        /// <summary>
        /// Determines whether one bind-address token represents wildcard-listener semantics.
        /// </summary>
        /// <param name="bindAddressValue">Bind-address token text.</param>
        /// <returns><see langword="true"/> when token is <c>*</c> or <c>Any</c>; otherwise <see langword="false"/>.</returns>
        internal static bool IsWildcardBindAddressToken(string? bindAddressValue)
        {
            if (string.IsNullOrWhiteSpace(bindAddressValue))
            {
                return false;
            }

            string trimmedValue = bindAddressValue.Trim();
            return string.Equals(trimmedValue, "*", StringComparison.Ordinal)
                || string.Equals(trimmedValue, "Any", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Enumerates local unicast interface addresses from operational interfaces.
        /// </summary>
        /// <returns>Collected local interface addresses.</returns>
        /// <exception cref="InvalidOperationException">Thrown when interface enumeration fails.</exception>
        private static IReadOnlyList<IPAddress> EnumerateInterfaceAddresses()
        {
            List<IPAddress> interfaceAddresses = [];

            try
            {
                foreach (NetworkInterface networkInterface in NetworkInterface.GetAllNetworkInterfaces())
                {
                    if (networkInterface.OperationalStatus != OperationalStatus.Up)
                    {
                        continue;
                    }

                    IPInterfaceProperties ipInterfaceProperties = networkInterface.GetIPProperties();
                    foreach (UnicastIPAddressInformation unicastAddress in ipInterfaceProperties.UnicastAddresses)
                    {
                        interfaceAddresses.Add(unicastAddress.Address);
                    }
                }

                return interfaceAddresses;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Unable to enumerate local network interfaces for wildcard BindAddress DNS derivation: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Determines whether an address is eligible for DNS advertisement.
        /// </summary>
        /// <param name="address">Address to evaluate.</param>
        /// <returns><see langword="true"/> when address is eligible for DNS advertisement.</returns>
        private static bool IsEligibleDnsAddress(IPAddress address)
        {
            ArgumentNullException.ThrowIfNull(address);

            if (address.AddressFamily is not (AddressFamily.InterNetwork or AddressFamily.InterNetworkV6))
            {
                return false;
            }

            if (IPAddress.IsLoopback(address)
                || IPAddress.Any.Equals(address)
                || IPAddress.IPv6Any.Equals(address)
                || IPAddress.None.Equals(address)
                || IPAddress.IPv6None.Equals(address))
            {
                return false;
            }

            if (address.AddressFamily == AddressFamily.InterNetwork)
            {
                byte[] ipv4Bytes = address.GetAddressBytes();
                bool isIpv4LinkLocal = ipv4Bytes[0] == 169 && ipv4Bytes[1] == 254;
                bool isIpv4Multicast = ipv4Bytes[0] is >= 224 and <= 239;
                return !isIpv4LinkLocal && !isIpv4Multicast;
            }

            return !address.IsIPv6LinkLocal && !address.IsIPv6Multicast;
        }

        /// <summary>
        /// Canonicalizes and deterministically orders address values.
        /// </summary>
        /// <param name="addresses">Addresses to canonicalize.</param>
        /// <returns>Canonical deterministic address set.</returns>
        private static IReadOnlyList<IPAddress> CanonicalizeAddresses(IEnumerable<IPAddress> addresses)
        {
            ArgumentNullException.ThrowIfNull(addresses);

            HashSet<IPAddress> uniqueAddresses = [.. addresses];
            return [.. uniqueAddresses
                .OrderBy(static address => address.AddressFamily)
                .ThenBy(static address => address.GetAddressBytes(), ByteSequenceComparer.Instance)];
        }

        /// <summary>
        /// Lexicographically compares byte sequences for deterministic address ordering.
        /// </summary>
        private sealed class ByteSequenceComparer : IComparer<byte[]>
        {
            /// <summary>
            /// Singleton comparer instance.
            /// </summary>
            internal static ByteSequenceComparer Instance { get; } = new();

            /// <summary>
            /// Compares two byte arrays lexicographically.
            /// </summary>
            /// <param name="x">First byte array.</param>
            /// <param name="y">Second byte array.</param>
            /// <returns>Comparison result.</returns>
            public int Compare(byte[]? x, byte[]? y)
            {
                if (ReferenceEquals(x, y))
                {
                    return 0;
                }

                if (x is null)
                {
                    return -1;
                }

                if (y is null)
                {
                    return 1;
                }

                int minimumLength = Math.Min(x.Length, y.Length);
                for (int index = 0; index < minimumLength; index++)
                {
                    int comparison = x[index].CompareTo(y[index]);
                    if (comparison != 0)
                    {
                        return comparison;
                    }
                }

                return x.Length.CompareTo(y.Length);
            }
        }
    }
}
