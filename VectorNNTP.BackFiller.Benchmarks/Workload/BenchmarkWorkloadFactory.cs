using System.Diagnostics;
using System.Text;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class BenchmarkWorkloadFactory
{
    private const int PreGeneratedMessageIdPoolSize = 2_000_000;

    internal static PreparedBenchmarkWorkload PrepareBenchmarkWorkload(TransitBenchmarkConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        Stopwatch idGenerationStopwatch = Stopwatch.StartNew();
        string[] messageIds = new string[PreGeneratedMessageIdPoolSize];
        HashSet<string> uniqueness = new(PreGeneratedMessageIdPoolSize, StringComparer.Ordinal);

        for (int i = 0; i < messageIds.Length; i++)
        {
            string messageId = TransitBenchmarkCore.BuildMessageId(config.BenchmarkInstanceId, workerId: 0, sequence: i + 1, phase: "pre");
            messageIds[i] = messageId;
            uniqueness.Add(messageId);
        }

        idGenerationStopwatch.Stop();

        Stopwatch payloadPreparationStopwatch = Stopwatch.StartNew();
        byte[] reusablePayloadTemplate = CreateReusablePayloadTemplate(config.ArticleTargetBytes);
        payloadPreparationStopwatch.Stop();

        int duplicateCount = messageIds.Length - uniqueness.Count;

        WorkloadPreparationSummary summary = new(
            PreGenerationDurationMilliseconds: idGenerationStopwatch.Elapsed.TotalMilliseconds,
            PayloadPreparationDurationMilliseconds: payloadPreparationStopwatch.Elapsed.TotalMilliseconds,
            MessageIdPoolSize: messageIds.Length,
            UniqueMessageIdCount: uniqueness.Count,
            DuplicateMessageIdCount: duplicateCount,
            ReusablePayloadBytes: reusablePayloadTemplate.Length);

        Console.WriteLine($"Pre-generation duration ms: {summary.PreGenerationDurationMilliseconds:F2}");
        Console.WriteLine($"Pre-generated Message-IDs: {summary.MessageIdPoolSize:N0}");
        Console.WriteLine($"Unique Message-IDs: {summary.UniqueMessageIdCount:N0}");
        Console.WriteLine($"Duplicate Message-IDs: {summary.DuplicateMessageIdCount:N0}");
        Console.WriteLine($"Payload preparation duration ms: {summary.PayloadPreparationDurationMilliseconds:F2}");
        Console.WriteLine($"Reusable article payload bytes: {summary.ReusablePayloadBytes:N0}");

        return new PreparedBenchmarkWorkload(messageIds, reusablePayloadTemplate, summary);
    }

    private static byte[] CreateReusablePayloadTemplate(int targetBytes)
    {
        string headers = "Message-ID: <benchmark-static@usenet.ninja>\r\n" +
                         "Date: Thu, 01 Jan 1970 00:00:00 GMT\r\n" +
                         "From: benchmark@usenet.ninja\r\n" +
                         "Newsgroups: alt.test\r\n" +
                         "Subject: BackFiller TransitPublisher benchmark workload\r\n" +
                         "Path: benchmark.usenet.ninja\r\n" +
                         "\r\n";

        byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
        int minimumTrailerBytes = 2;
        int bodyBytes = Math.Max(1, targetBytes - headerBytes.Length - minimumTrailerBytes);

        byte[] reusable = new byte[headerBytes.Length + bodyBytes + minimumTrailerBytes];
        int offset = 0;

        Buffer.BlockCopy(headerBytes, 0, reusable, offset, headerBytes.Length);
        offset += headerBytes.Length;

        for (int i = 0; i < bodyBytes; i++)
        {
            reusable[offset++] = (byte)('a' + (i % 26));
        }

        reusable[offset++] = (byte)'\r';
        reusable[offset++] = (byte)'\n';

        return reusable;
    }
}
