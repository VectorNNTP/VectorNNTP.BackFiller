// <copyright file="BindAddressDnsAddressDeriverTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using System.Net;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests canonical DNS-address derivation from explicit and wildcard BindAddress semantics.
    /// </summary>
    public sealed class BindAddressDnsAddressDeriverTests
    {
        /// <summary>
        /// Verifies explicit IPv4 and IPv6 bind addresses are parsed and preserved exactly.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenExplicitIpv4AndIpv6Configured_ReturnsExplicitAddressSet()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["198.51.100.10", "2001:db8::10", "2001:0db8:0:0:0:0:0:10"]);

            Assert.Equal(
                ["198.51.100.10", "2001:db8::10"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard '*' derives DNS addresses from eligible local interfaces and excludes loopback, unspecified, and link-local addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardAsteriskConfigured_UsesEligibleInterfaceAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["*"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.23.45.67"),
                    IPAddress.Parse("172.17.5.9"),
                    IPAddress.Parse("192.168.50.20"),
                    IPAddress.Parse("198.51.100.77"),
                    IPAddress.Parse("2001:db8::10"),
                    IPAddress.Parse("2001:db8::11"),
                    IPAddress.Parse("127.0.0.1"),
                    IPAddress.Parse("::1"),
                    IPAddress.Any,
                    IPAddress.IPv6Any,
                    IPAddress.Parse("169.254.1.20"),
                    IPAddress.Parse("fe80::1234"),
                ]);

            Assert.Equal(
                ["10.23.45.67", "172.17.5.9", "192.168.50.20", "198.51.100.77", "2001:db8::10", "2001:db8::11"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard 'Any' token derives DNS addresses from eligible local interfaces.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardAnyConfigured_UsesEligibleInterfaceAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["Any"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.0.0.50"),
                    IPAddress.Parse("172.17.0.9"),
                    IPAddress.Parse("2001:db8::99"),
                    IPAddress.Loopback,
                    IPAddress.IPv6Loopback,
                    IPAddress.Parse("fe80::abcd"),
                ]);

            Assert.Equal(
                ["10.0.0.50", "172.17.0.9", "2001:db8::99"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies IPv4 wildcard listener address 0.0.0.0 derives only eligible local IPv4 interface addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenIpv4WildcardConfigured_UsesEligibleIpv4InterfaceAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.11.12.13"),
                    IPAddress.Parse("172.17.9.7"),
                    IPAddress.Parse("2001:db8::100"),
                    IPAddress.Parse("169.254.44.10"),
                    IPAddress.Parse("127.0.0.1"),
                    IPAddress.Any,
                ]);

            Assert.Equal(
                ["10.11.12.13", "172.17.9.7"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies IPv6 wildcard listener address :: derives only eligible local IPv6 interface addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenIpv6WildcardConfigured_UsesEligibleIpv6InterfaceAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["::"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.11.12.13"),
                    IPAddress.Parse("2001:db8::200"),
                    IPAddress.Parse("2001:db8::201"),
                    IPAddress.Parse("fe80::beef"),
                    IPAddress.Parse("::1"),
                    IPAddress.IPv6Any,
                ]);

            Assert.Equal(
                ["2001:db8::200", "2001:db8::201"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies combined wildcard markers derive the union of required address-family interface addresses plus explicit addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenMixedWildcardAndExplicitConfigured_ReturnsUnionWithoutWildcardLiterals()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0", "::", "Any", "198.51.100.9", "2001:db8::9"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.0.0.9"),
                    IPAddress.Parse("172.17.8.4"),
                    IPAddress.Parse("2001:db8::40"),
                    IPAddress.Parse("2001:db8::41"),
                    IPAddress.Parse("0.0.0.0"),
                    IPAddress.Parse("::"),
                    IPAddress.Parse("127.0.0.1"),
                    IPAddress.Parse("::1"),
                    IPAddress.Parse("fe80::10"),
                ]);

            string[] canonicalAddresses = [.. addresses.Select(static address => address.ToString())];

            Assert.Contains("198.51.100.9", canonicalAddresses);
            Assert.Contains("2001:db8::9", canonicalAddresses);
            Assert.Contains("10.0.0.9", canonicalAddresses);
            Assert.Contains("172.17.8.4", canonicalAddresses);
            Assert.Contains("2001:db8::40", canonicalAddresses);
            Assert.Contains("2001:db8::41", canonicalAddresses);
            Assert.DoesNotContain("0.0.0.0", canonicalAddresses);
            Assert.DoesNotContain("::", canonicalAddresses);
            Assert.DoesNotContain("127.0.0.1", canonicalAddresses);
            Assert.DoesNotContain("::1", canonicalAddresses);
        }

        /// <summary>
        /// Verifies explicit non-wildcard addresses that cannot be advertised are excluded from DNS derivation.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenExplicitAddressIsNonAdvertisable_ExcludesAddress()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["127.0.0.1", "::1", "fe80::1234", "224.1.1.1", "198.51.100.99"]);

            Assert.Equal(["198.51.100.99"], [.. addresses.Select(static address => address.ToString())]);
        }

        /// <summary>
        /// Verifies IPv4 and IPv6 wildcard literals are interpreted as wildcard semantics and never published as DNS addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardLiteralsConfigured_DoesNotReturnWildcardAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0", "::"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Any,
                    IPAddress.IPv6Any,
                    IPAddress.Parse("10.99.0.5"),
                    IPAddress.Parse("2001:db8::500"),
                ]);

            string[] canonicalAddresses = [.. addresses.Select(static address => address.ToString())];

            Assert.DoesNotContain("0.0.0.0", canonicalAddresses);
            Assert.DoesNotContain("::", canonicalAddresses);
            Assert.Contains("10.99.0.5", canonicalAddresses);
            Assert.Contains("2001:db8::500", canonicalAddresses);
        }

        /// <summary>
        /// Verifies wildcard + explicit family-specific wildcard values derive union based on configured wildcard semantics.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenMultipleWildcardValuesConfigured_DerivesUnionOfEligibleAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0", "::"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.50.1.1"),
                    IPAddress.Parse("172.17.1.9"),
                    IPAddress.Parse("2001:db8::700"),
                    IPAddress.Parse("2001:db8::701"),
                ]);

            Assert.Equal(
                ["10.50.1.1", "172.17.1.9", "2001:db8::700", "2001:db8::701"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard token plus IPv4 wildcard literal still produces union of both address families.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenAnyAndIpv4WildcardConfigured_DerivesBothAddressFamilies()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["Any", "0.0.0.0"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.200.1.1"),
                    IPAddress.Parse("2001:db8::900"),
                ]);

            Assert.Equal(
                ["10.200.1.1", "2001:db8::900"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies IPv4 and IPv6 wildcard literals are not treated as wildcard token strings.
        /// </summary>
        [Fact]
        public void IsWildcardBindAddressToken_WhenWildcardIpLiteralProvided_ReturnsFalse()
        {
            Assert.False(BindAddressDnsAddressDeriver.IsWildcardBindAddressToken("0.0.0.0"));
            Assert.False(BindAddressDnsAddressDeriver.IsWildcardBindAddressToken("::"));
        }

        /// <summary>
        /// Verifies explicit wildcard-family semantics with an explicit address derive the expected union.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenIpv4WildcardAndExplicitIpv6Configured_ReturnsExpectedUnion()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0", "2001:db8::888"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.77.1.2"),
                    IPAddress.Parse("172.17.2.3"),
                    IPAddress.Parse("2001:db8::123"),
                ]);

            Assert.Equal(
                ["10.77.1.2", "172.17.2.3", "2001:db8::888"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies explicit wildcard-family semantics with an explicit IPv4 address derive the expected union.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenIpv6WildcardAndExplicitIpv4Configured_ReturnsExpectedUnion()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["::", "198.51.100.44"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("2001:db8::321"),
                    IPAddress.Parse("2001:db8::322"),
                    IPAddress.Parse("10.77.1.2"),
                ]);

            Assert.Equal(
                ["198.51.100.44", "2001:db8::321", "2001:db8::322"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard-only derivation excludes loopback, unspecified, link-local, and multicast addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardConfigured_ExcludesNonAdvertisableInterfaceAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["Any"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Loopback,
                    IPAddress.IPv6Loopback,
                    IPAddress.Any,
                    IPAddress.IPv6Any,
                    IPAddress.Parse("169.254.10.20"),
                    IPAddress.Parse("fe80::1"),
                    IPAddress.Parse("224.0.0.5"),
                    IPAddress.Parse("ff02::1"),
                    IPAddress.Parse("10.1.1.1"),
                    IPAddress.Parse("2001:db8::77"),
                ]);

            Assert.Equal(
                ["10.1.1.1", "2001:db8::77"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard and explicit address duplication is deduplicated in final canonical output.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardAndExplicitOverlap_DeduplicatesOutput()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["Any", "10.9.9.9", "2001:db8::909"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.9.9.9"),
                    IPAddress.Parse("2001:db8::909"),
                    IPAddress.Parse("172.17.7.7"),
                ]);

            Assert.Equal(
                ["10.9.9.9", "172.17.7.7", "2001:db8::909"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard configuration still allows explicit RFC1918, Docker, and VPN addresses through exact semantics.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenExplicitRfc1918AndWildcardConfigured_IncludesExpectedAddresses()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["10.8.0.15", "172.17.0.10", "Any"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Parse("10.8.0.15"),
                    IPAddress.Parse("172.17.0.10"),
                    IPAddress.Parse("192.168.2.55"),
                    IPAddress.Parse("2001:db8::505"),
                ]);

            Assert.Equal(
                ["10.8.0.15", "172.17.0.10", "192.168.2.55", "2001:db8::505"],
                addresses.Select(static address => address.ToString()).OrderBy(static value => value, StringComparer.Ordinal));
        }

        /// <summary>
        /// Verifies wildcard-family-only semantics can derive an empty set when no eligible interfaces are present.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardConfiguredAndNoEligibleInterfaces_ReturnsEmptySet()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["0.0.0.0", "::"],
                interfaceAddressProvider: static () =>
                [
                    IPAddress.Any,
                    IPAddress.IPv6Any,
                    IPAddress.Loopback,
                    IPAddress.IPv6Loopback,
                    IPAddress.Parse("fe80::10"),
                ]);

            Assert.Empty(addresses);
        }

        /// <summary>
        /// Verifies wildcard semantics do not perform hostname DNS resolution and only depend on provided interface addresses.
        /// </summary>
        [Fact]
        public void DeriveCanonicalDnsAddresses_WhenWildcardConfigured_UsesProvidedInterfaceSourceOnly()
        {
            IReadOnlyList<IPAddress> addresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(
                ["Any"],
                interfaceAddressProvider: static () => [IPAddress.Parse("10.123.45.67")]);

            Assert.Equal(["10.123.45.67"], [.. addresses.Select(static address => address.ToString())]);
        }
        /// <summary>
        /// Verifies wildcard token detection recognizes '*' and 'Any' values without requiring IP parsing.
        /// </summary>
        [Theory]
        [InlineData("*")]
        [InlineData("Any")]
        [InlineData("any")]
        [InlineData(" Any ")]
        public void IsWildcardBindAddressToken_WhenWildcardTokenProvided_ReturnsTrue(string value)
        {
            bool isWildcardToken = BindAddressDnsAddressDeriver.IsWildcardBindAddressToken(value);

            Assert.True(isWildcardToken);
        }

        /// <summary>
        /// Verifies non-wildcard values are not treated as wildcard bind-address tokens.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("127.0.0.1")]
        [InlineData("0.0.0.0")]
        [InlineData("::")]
        public void IsWildcardBindAddressToken_WhenNonWildcardValueProvided_ReturnsFalse(string? value)
        {
            bool isWildcardToken = BindAddressDnsAddressDeriver.IsWildcardBindAddressToken(value);

            Assert.False(isWildcardToken);
        }
    }
}
