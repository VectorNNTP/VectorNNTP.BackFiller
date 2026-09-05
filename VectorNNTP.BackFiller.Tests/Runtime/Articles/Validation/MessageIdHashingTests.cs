// <copyright file="MessageIdHashingTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Validation
// Focused tests for canonical Message-ID hashing used by article cache URI identity.

using System.Security.Cryptography;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Validation
{
    /// <summary>
    /// Verifies canonical Message-ID MD5 hashing contract semantics.
    /// </summary>
    public sealed class MessageIdHashingTests
    {
        /// <summary>
        /// Confirms the canonical deterministic vector for the repository Message-ID sample 12345@example.invalid.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenMessageIdIs12345Vector_ReturnsExpectedDigest()
        {
            string digest = MessageIdHashing.ComputeCanonicalMd5Hex("<12345@example.invalid>");

            Assert.Equal("30edc94157aa16fe644a45a1f1ffe160", digest);
        }

        /// <summary>
        /// Confirms the canonical deterministic vector for the repository Message-ID sample abc@example.invalid.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenMessageIdIsAbcVector_ReturnsExpectedDigest()
        {
            string digest = MessageIdHashing.ComputeCanonicalMd5Hex("<abc@example.invalid>");

            Assert.Equal("de438dc83d64b1fa9206cf4da9eed5cc", digest);
        }

        /// <summary>
        /// Confirms angle brackets are part of the hash input and therefore change the resulting digest.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenAngleBracketsRemoved_ProducesDifferentDigest()
        {
            string withBrackets = MessageIdHashing.ComputeCanonicalMd5Hex("<abc@example.invalid>");
            string withoutBrackets = MessageIdHashing.ComputeCanonicalMd5Hex("abc@example.invalid");

            Assert.Equal("de438dc83d64b1fa9206cf4da9eed5cc", withBrackets);
            Assert.Equal("a7ee85c34e58bc015f147f7c2bfbe85c", withoutBrackets);
            Assert.NotEqual(withBrackets, withoutBrackets);
        }

        /// <summary>
        /// Confirms Message-ID letter casing is preserved as-is for hash input.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenMessageIdCaseDiffers_ProducesDifferentDigest()
        {
            string lower = MessageIdHashing.ComputeCanonicalMd5Hex("<abc@example.invalid>");
            string upper = MessageIdHashing.ComputeCanonicalMd5Hex("<ABC@example.invalid>");

            Assert.Equal("de438dc83d64b1fa9206cf4da9eed5cc", lower);
            Assert.Equal("983057a19b5437d0330045ac8c546d67", upper);
            Assert.NotEqual(lower, upper);
        }

        /// <summary>
        /// Confirms no whitespace normalization is applied before hashing.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenWhitespaceDiffers_ProducesDifferentDigest()
        {
            string canonical = MessageIdHashing.ComputeCanonicalMd5Hex("<abc@example.invalid>");
            string leadingSpace = MessageIdHashing.ComputeCanonicalMd5Hex(" <abc@example.invalid>");
            string trailingSpace = MessageIdHashing.ComputeCanonicalMd5Hex("<abc@example.invalid> ");

            Assert.Equal("de438dc83d64b1fa9206cf4da9eed5cc", canonical);
            Assert.Equal("982dac3a223f8cbcb0d95b775ae63c81", leadingSpace);
            Assert.Equal("7e3527bd2557dd1650860adf49ff9b49", trailingSpace);
            Assert.NotEqual(canonical, leadingSpace);
            Assert.NotEqual(canonical, trailingSpace);
        }

        /// <summary>
        /// Confirms the helper uses ASCII bytes for digest input rather than UTF-8 bytes.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenInputContainsNonAscii_UsesAsciiByteEncoding()
        {
            const string messageId = "<abcé@example.invalid>";
            string digest = MessageIdHashing.ComputeCanonicalMd5Hex(messageId);

            string utf8Digest = Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(messageId))).ToLowerInvariant();

            Assert.Equal("c107584a75b6df23bf3b4e6a87f30c27", digest);
            Assert.Equal("d0969aee52c6d73faa0231400ba21187", utf8Digest);
            Assert.NotEqual(utf8Digest, digest);
        }

        /// <summary>
        /// Confirms digest format is exactly 32 lowercase hexadecimal characters.
        /// </summary>
        [Fact]
        public void ComputeCanonicalMd5Hex_WhenMessageIdValid_ReturnsLowercase32HexCharacters()
        {
            string digest = MessageIdHashing.ComputeCanonicalMd5Hex("<12345@example.invalid>");

            Assert.Equal(32, digest.Length);
            Assert.All(digest, ch => Assert.True((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')));
        }
    }
}
