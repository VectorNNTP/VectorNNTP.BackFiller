// <copyright file="TransitDotStuffingTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit dot stuffing, covering NNTP article and transport behavior.

using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Covers transit dot stuffing behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class TransitDotStuffingTests
    {
        /// <summary>
        /// Exercises payload cases behavior, including the expected result and failure semantics.
        /// </summary>
        public static IEnumerable<object[]> PayloadCases()
        {
            yield return ["empty", Array.Empty<byte>()];
            yield return ["one-byte", new byte[] { 0x41 }];
            yield return ["single-lf", new byte[] { (byte)'\n' }];
            yield return ["single-cr", new byte[] { (byte)'\r' }];
            yield return ["crlf", new byte[] { (byte)'\r', (byte)'\n' }];
            yield return ["lf-only-lines", new byte[] { (byte)'A', (byte)'\n', (byte)'B', (byte)'\n' }];
            yield return ["line-begins-dot", new byte[] { (byte)'.', (byte)'a', (byte)'\n' }];
            yield return ["multiple-dot-lines", new byte[] { (byte)'.', (byte)'a', (byte)'\n', (byte)'.', (byte)'b', (byte)'\n' }];
            yield return ["consecutive-dot-lines", new byte[] { (byte)'.', (byte)'\n', (byte)'.', (byte)'\n', (byte)'.', (byte)'\n' }];
            yield return ["empty-lines", new byte[] { (byte)'\n', (byte)'\n', (byte)'\n' }];
            yield return ["contains-null", new byte[] { 0x00, 0x01, 0x02, (byte)'\n', (byte)'.', 0x00, (byte)'\n' }];
            yield return ["not-ending-lf", new byte[] { (byte)'A', (byte)'\r', (byte)'B' }];
            yield return ["all-256-values", BuildAllByteValuesPayload()];
            yield return ["mixed-realistic", BuildMixedPayload(128 * 1024, seed: 17, dotStartEvery: 11, averageLineLength: 160)];
            yield return ["large-random-binary", BuildRandomPayload(512 * 1024, seed: 23)];
            yield return ["payload-2mib", BuildMixedPayload(2_097_152, seed: 29, dotStartEvery: 19, averageLineLength: 300)];
        }
        /// <summary>
        /// Exercises try dot stuff  all algorithms  match reference behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [MemberData(nameof(PayloadCases))]
        public void TryDotStuff_AllAlgorithms_MatchReference(string _, byte[] payload)
        {
            byte[] expected = ReferenceDotStuff(payload, appendTrailingCrlfWhenMissingLf: true);

            foreach (TransitDotStuffingAlgorithm algorithm in Enum.GetValues<TransitDotStuffingAlgorithm>())
            {
                int required = TransitDotStuffing.GetRequiredDestinationLength(payload, appendTrailingCrlfWhenMissingLf: true, out int requiredStuffedDots);
                Assert.Equal(expected.Length, required);

                byte[] destination = new byte[required];
                bool ok = TransitDotStuffing.TryDotStuff(payload, destination, out TransitDotStuffTransformResult result, algorithm, appendTrailingCrlfWhenMissingLf: true);
                Assert.True(ok, $"TryDotStuff returned false for {algorithm}");
                Assert.Equal(expected.Length, result.BytesWritten);
                Assert.Equal(requiredStuffedDots, result.StuffedDotCount);
                Assert.Equal(expected, destination);
            }
        }
        /// <summary>
        /// Exercises try dot stuff  without trailing crlf append  matches reference behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [MemberData(nameof(PayloadCases))]
        public void TryDotStuff_WithoutTrailingCrlfAppend_MatchesReference(string _, byte[] payload)
        {
            byte[] expected = ReferenceDotStuff(payload, appendTrailingCrlfWhenMissingLf: false);

            foreach (TransitDotStuffingAlgorithm algorithm in Enum.GetValues<TransitDotStuffingAlgorithm>())
            {
                int required = TransitDotStuffing.GetRequiredDestinationLength(payload, appendTrailingCrlfWhenMissingLf: false, out int requiredStuffedDots);
                Assert.Equal(expected.Length, required);

                byte[] destination = new byte[required];
                bool ok = TransitDotStuffing.TryDotStuff(payload, destination, out TransitDotStuffTransformResult result, algorithm, appendTrailingCrlfWhenMissingLf: false);
                Assert.True(ok, $"TryDotStuff returned false for {algorithm}");
                Assert.Equal(expected.Length, result.BytesWritten);
                Assert.Equal(requiredStuffedDots, result.StuffedDotCount);
                Assert.Equal(expected, destination);
            }
        }
        /// <summary>
        /// Exercises try dot stuff  when destination too small  returns false behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void TryDotStuff_WhenDestinationTooSmall_ReturnsFalse()
        {
            byte[] payload = [(byte)'.', (byte)'a', (byte)'\n', (byte)'b'];

            foreach (TransitDotStuffingAlgorithm algorithm in Enum.GetValues<TransitDotStuffingAlgorithm>())
            {
                int required = TransitDotStuffing.GetRequiredDestinationLength(payload, appendTrailingCrlfWhenMissingLf: true, out _);
                byte[] destination = new byte[required - 1];
                bool ok = TransitDotStuffing.TryDotStuff(payload, destination, out _, algorithm, appendTrailingCrlfWhenMissingLf: true);
                Assert.False(ok);
            }
        }

        /// <summary>
        /// Exercises build all byte values payload behavior, including the expected result and failure semantics.
        /// </summary>
        private static byte[] BuildAllByteValuesPayload()
        {
            byte[] bytes = new byte[256 + 32];
            for (int i = 0; i < 256; i++)
            {
                bytes[i] = (byte)i;
            }

            bytes[256] = (byte)'\n';
            bytes[257] = (byte)'.';
            bytes[258] = (byte)'x';
            bytes[259] = (byte)'\n';
            bytes[^1] = (byte)'\n';
            return bytes;
        }

        /// <summary>
        /// Exercises build random payload behavior, including the expected result and failure semantics.
        /// </summary>
        private static byte[] BuildRandomPayload(int size, int seed)
        {
            byte[] data = new byte[size];
            Random random = new(seed);
            random.NextBytes(data);
            if (size > 0)
            {
                data[size / 2] = (byte)'\n';
                data[^1] = (byte)'\n';
            }

            return data;
        }

        /// <summary>
        /// Exercises build mixed payload behavior, including the expected result and failure semantics.
        /// </summary>
        private static byte[] BuildMixedPayload(int size, int seed, int dotStartEvery, int averageLineLength)
        {
            byte[] data = new byte[size];
            Random random = new(seed);
            int index = 0;
            int line = 0;
            while (index < size)
            {
                int lineLength = Math.Max(1, averageLineLength + random.Next(-averageLineLength / 2, averageLineLength / 2));
                bool dotStart = dotStartEvery > 0 && (line % dotStartEvery == 0);
                for (int i = 0; i < lineLength && index < size; i++)
                {
                    byte b;
                    if (i == 0 && dotStart)
                    {
                        b = (byte)'.';
                    }
                    else
                    {
                        int v = random.Next(1, 255);
                        b = v == (byte)'\n' ? (byte)'X' : (byte)v;
                    }

                    data[index++] = b;
                }

                if (index < size)
                {
                    data[index++] = (byte)'\n';
                }

                line++;
            }

            if (size > 0)
            {
                data[^1] = (byte)'\n';
            }

            return data;
        }

        /// <summary>
        /// Exercises reference dot stuff behavior, including the expected result and failure semantics.
        /// </summary>
        private static byte[] ReferenceDotStuff(ReadOnlySpan<byte> source, bool appendTrailingCrlfWhenMissingLf)
        {
            List<byte> output = new(source.Length + 64);
            bool atLineStart = true;

            for (int i = 0; i < source.Length; i++)
            {
                byte current = source[i];

                if (atLineStart && current == (byte)'.')
                {
                    output.Add((byte)'.');
                }

                output.Add(current);
                atLineStart = current == (byte)'\n';
            }

            if (appendTrailingCrlfWhenMissingLf && source.Length > 0 && source[^1] != (byte)'\n')
            {
                output.Add((byte)'\r');
                output.Add((byte)'\n');
            }

            return [.. output];
        }
    }
}
