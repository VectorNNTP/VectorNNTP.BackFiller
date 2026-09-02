// <copyright file="NntpArticleAcquisitionCorpusGenerationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp article acquisition corpus generation, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp article acquisition corpus generation test suite.

using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Provides opt-in local fixture generation for real-world acquisition and parser compatibility validation.
    /// </summary>
    public sealed class NntpArticleAcquisitionCorpusGenerationTests
    {
        /// <summary>
        /// Name of environment variable that enables local corpus generation.
        /// </summary>
        private const string EnableCorpusGenerationEnvironmentVariable = "VNNTP_ENABLE_LOCAL_CORPUS_GENERATION";

        /// <summary>
        /// Name of environment variable containing remote NNTP hostname.
        /// </summary>
        private const string CorpusHostEnvironmentVariable = "VNNTP_CORPUS_HOST";

        /// <summary>
        /// Name of environment variable containing remote NNTP port.
        /// </summary>
        private const string CorpusPortEnvironmentVariable = "VNNTP_CORPUS_PORT";

        /// <summary>
        /// Name of environment variable containing remote NNTP SSL setting.
        /// </summary>
        private const string CorpusUseSslEnvironmentVariable = "VNNTP_CORPUS_USE_SSL";

        /// <summary>
        /// Name of environment variable containing optional remote NNTP username.
        /// </summary>
        private const string CorpusUsernameEnvironmentVariable = "VNNTP_CORPUS_USERNAME";

        /// <summary>
        /// Name of environment variable containing optional remote NNTP password.
        /// </summary>
        private const string CorpusPasswordEnvironmentVariable = "VNNTP_CORPUS_PASSWORD";

        /// <summary>
        /// Name of environment variable containing newline-delimited Message-IDs for legitimate yEnc articles.
        /// </summary>
        private const string CorpusYEncMessageIdsEnvironmentVariable = "VNNTP_CORPUS_YENC_MESSAGE_IDS";

        /// <summary>
        /// Name of environment variable containing newline-delimited Message-IDs for legitimate non-yEnc articles.
        /// </summary>
        private const string CorpusPlainMessageIdsEnvironmentVariable = "VNNTP_CORPUS_PLAIN_MESSAGE_IDS";

        /// <summary>
        /// Repository-local corpus root relative to test project.
        /// </summary>
        private static readonly string CorpusRootDirectory = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "LocalCorpus"));

        /// <summary>
        /// Downloads configured corpus samples and writes deterministic fixture files locally.
        /// </summary>
        [Fact]
        public async Task GenerateLocalCorpus_WhenEnabled_DownloadsAndCorruptsConfiguredSamples()
        {
            if (!IsCorpusGenerationEnabled())
            {
                return;
            }

            NntpArticleAcquisitionEndpoint endpoint = ReadEndpointFromEnvironment();
            string[] yEncMessageIds = ReadRequiredMessageIds(CorpusYEncMessageIdsEnvironmentVariable);
            string[] plainMessageIds = ReadRequiredMessageIds(CorpusPlainMessageIdsEnvironmentVariable);

            _ = Directory.CreateDirectory(CorpusRootDirectory);
            string yEncDirectory = Path.Combine(CorpusRootDirectory, "yenc-valid");
            string plainDirectory = Path.Combine(CorpusRootDirectory, "plain-valid");
            string corruptDirectory = Path.Combine(CorpusRootDirectory, "yenc-corrupt");
            _ = Directory.CreateDirectory(yEncDirectory);
            _ = Directory.CreateDirectory(plainDirectory);
            _ = Directory.CreateDirectory(corruptDirectory);

            await DownloadAndWriteSamplesAsync(endpoint, yEncMessageIds, yEncDirectory, "yenc");
            await DownloadAndWriteSamplesAsync(endpoint, plainMessageIds, plainDirectory, "plain");
            BuildDeterministicCorruptVariants(yEncDirectory, corruptDirectory);

            string readmePath = Path.Combine(CorpusRootDirectory, "README.md");
            await File.WriteAllTextAsync(
                readmePath,
                BuildCorpusReadme(endpoint, yEncMessageIds.Length, plainMessageIds.Length),
                Encoding.UTF8);

            Assert.True(File.Exists(readmePath));
        }

        /// <summary>
        /// Returns a value indicating whether local corpus generation is enabled.
        /// </summary>
        /// <returns><see langword="true"/> when generation is enabled; otherwise <see langword="false"/>.</returns>
        private static bool IsCorpusGenerationEnabled()
        {
            string? enabled = Environment.GetEnvironmentVariable(EnableCorpusGenerationEnvironmentVariable);
            return string.Equals(enabled, "1", StringComparison.Ordinal) || string.Equals(enabled, "true", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Reads required endpoint configuration from environment variables.
        /// </summary>
        /// <returns>Endpoint descriptor for remote acquisition.</returns>
        private static NntpArticleAcquisitionEndpoint ReadEndpointFromEnvironment()
        {
            string host = Environment.GetEnvironmentVariable(CorpusHostEnvironmentVariable)
                ?? throw new InvalidOperationException($"{CorpusHostEnvironmentVariable} is required when corpus generation is enabled.");

            string? portRaw = Environment.GetEnvironmentVariable(CorpusPortEnvironmentVariable);
            int port = int.TryParse(portRaw, out int parsedPort) ? parsedPort : 119;

            string? useSslRaw = Environment.GetEnvironmentVariable(CorpusUseSslEnvironmentVariable);
            bool useSsl = string.Equals(useSslRaw, "1", StringComparison.Ordinal) || string.Equals(useSslRaw, "true", StringComparison.OrdinalIgnoreCase);

            string? username = Environment.GetEnvironmentVariable(CorpusUsernameEnvironmentVariable);
            string? password = Environment.GetEnvironmentVariable(CorpusPasswordEnvironmentVariable);

            return new NntpArticleAcquisitionEndpoint(host, port, useSsl, username, password);
        }

        /// <summary>
        /// Reads required newline-delimited Message-ID list from environment.
        /// </summary>
        /// <param name="environmentVariableName">Environment variable name.</param>
        /// <returns>Message-ID array.</returns>
        private static string[] ReadRequiredMessageIds(string environmentVariableName)
        {
            string raw = Environment.GetEnvironmentVariable(environmentVariableName)
                ?? throw new InvalidOperationException($"{environmentVariableName} is required when corpus generation is enabled.");

            string[] messageIds = [.. raw
                .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Where(static value => value.Length > 0)];

            return messageIds.Length == 0
                ? throw new InvalidOperationException($"{environmentVariableName} must include at least one Message-ID.")
                : messageIds;
        }

        /// <summary>
        /// Downloads and writes raw article fixtures to a target directory over one reusable authenticated session.
        /// </summary>
        /// <param name="endpoint">Remote endpoint descriptor.</param>
        /// <param name="messageIds">Message-IDs to download.</param>
        /// <param name="targetDirectory">Fixture output directory.</param>
        /// <param name="prefix">File prefix.</param>
        /// <returns>Completion task.</returns>
        private static async Task DownloadAndWriteSamplesAsync(
            NntpArticleAcquisitionEndpoint endpoint,
            IReadOnlyList<string> messageIds,
            string targetDirectory,
            string prefix)
        {
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            if (session is null)
            {
                throw new InvalidOperationException($"Failed to establish acquisition session: {connectResult.FailureCode} ({connectResult.ResponseCode}) {connectResult.ResponseText}");
            }

            await using (session.ConfigureAwait(false))
            {
                for (int i = 0; i < messageIds.Count; i++)
                {
                    string messageId = messageIds[i];

                    using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync(
                        messageId,
                        CancellationToken.None).ConfigureAwait(false);

                    if (!result.IsSuccess)
                    {
                        throw new InvalidOperationException($"Failed to download Message-ID {messageId}: {result.FailureCode} ({result.ResponseCode}) {result.ResponseText}");
                    }

                    string fixturePath = Path.Combine(targetDirectory, $"{prefix}-{i + 1:D2}.nntp");
                    await File.WriteAllBytesAsync(fixturePath, result.ArticleBytes.ToArray()).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Creates deterministic corrupted variants by flipping fixed bytes and appending malformed yEnc terminator content.
        /// </summary>
        /// <param name="validDirectory">Directory containing valid yEnc fixtures.</param>
        /// <param name="corruptDirectory">Target directory for corrupted fixtures.</param>
        private static void BuildDeterministicCorruptVariants(string validDirectory, string corruptDirectory)
        {
            string[] files = Directory.GetFiles(validDirectory, "*.nntp", SearchOption.TopDirectoryOnly);
            Array.Sort(files, StringComparer.Ordinal);

            for (int i = 0; i < files.Length; i++)
            {
                byte[] bytes = File.ReadAllBytes(files[i]);
                byte[] corrupted = CorruptDeterministically(bytes, i + 1);
                string outputPath = Path.Combine(corruptDirectory, $"yenc-corrupt-{i + 1:D2}.nntp");
                File.WriteAllBytes(outputPath, corrupted);
            }
        }

        /// <summary>
        /// Applies deterministic byte-level corruption to one article fixture.
        /// </summary>
        /// <param name="source">Valid source fixture bytes.</param>
        /// <param name="seed">Deterministic seed value derived from file index.</param>
        /// <returns>Corrupted fixture bytes.</returns>
        private static byte[] CorruptDeterministically(byte[] source, int seed)
        {
            byte[] output = new byte[source.Length + 16];
            Buffer.BlockCopy(source, 0, output, 0, source.Length);

            using SHA256 hash = SHA256.Create();
            byte[] hashBytes = hash.ComputeHash(BitConverter.GetBytes(seed));

            for (int i = 0; i < 8 && i < source.Length; i++)
            {
                int index = (hashBytes[i] + (i * 31)) % source.Length;
                output[index] ^= (byte)(0x20 + i);
            }

            byte[] suffix = "\r\n=yend size=broken crc32=ZZZZZZZZ\r\n"u8.ToArray();
            Buffer.BlockCopy(suffix, 0, output, source.Length, suffix.Length);
            return output;
        }

        /// <summary>
        /// Builds corpus README content documenting opt-in fixture generation workflow.
        /// </summary>
        /// <param name="endpoint">Remote endpoint used for generation.</param>
        /// <param name="yEncCount">Number of yEnc source Message-IDs.</param>
        /// <param name="plainCount">Number of plain source Message-IDs.</param>
        /// <returns>README markdown content.</returns>
        private static string BuildCorpusReadme(NntpArticleAcquisitionEndpoint endpoint, int yEncCount, int plainCount)
        {
            return $"""
# Local NNTP Corpus (Development-Only)

This directory is generated locally and is excluded from source control.

- Host: {endpoint.Host}:{endpoint.Port}
- SSL: {endpoint.UseSsl}
- Valid yEnc samples downloaded: {yEncCount}
- Valid non-yEnc samples downloaded: {plainCount}
- Corrupted yEnc samples generated deterministically from valid yEnc fixtures.

## Generation Steps

1. Set environment variables:
   - `{EnableCorpusGenerationEnvironmentVariable}=1`
   - `{CorpusHostEnvironmentVariable}`
   - `{CorpusPortEnvironmentVariable}` (optional; default `119`)
   - `{CorpusUseSslEnvironmentVariable}` (optional; `true/false`)
   - `{CorpusUsernameEnvironmentVariable}` / `{CorpusPasswordEnvironmentVariable}` (optional)
   - `{CorpusYEncMessageIdsEnvironmentVariable}` (newline-delimited `<message-id>` list)
   - `{CorpusPlainMessageIdsEnvironmentVariable}` (newline-delimited `<message-id>` list)
2. Run the single test:
   - `dotnet test VectorNNTP.BackFiller.Tests --filter FullyQualifiedName~NntpArticleAcquisitionCorpusGenerationTests.GenerateLocalCorpus_WhenEnabled_DownloadsAndCorruptsConfiguredSamples`
3. Use generated fixtures in `LocalCorpus/yenc-valid`, `LocalCorpus/plain-valid`, and `LocalCorpus/yenc-corrupt`.

## CI Behavior

CI does not set `{EnableCorpusGenerationEnvironmentVariable}`, so this test exits immediately and does not require private NNTP infrastructure.
""";
        }
    }
}
