// <copyright file="TransitDotStuffingBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// TransitDotStuffingBenchmarks: compares dot-stuffing implementations across representative payload layouts.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using System.Buffers;
using System.IO.Pipelines;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Compares baseline and bulk NNTP dot-stuffing algorithms over representative payload distributions.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class TransitDotStuffingBenchmarks
{
    /// <summary>
    /// Gets or sets the payload Size.
    /// </summary>
    private const int PayloadSize = 2_097_152;

    /// <summary>
    /// Gets or sets the _source.
    /// </summary>
    private byte[] _source = null!;
    /// <summary>
    /// Gets or sets the _destination.
    /// </summary>
    private byte[] _destination = null!;

    /// <summary>
    /// Gets or sets the distribution.
    /// </summary>
    [Params(
        DotPayloadDistribution.NormalNoDot,
        DotPayloadDistribution.DotHeavy,
        DotPayloadDistribution.Mixed,
        DotPayloadDistribution.LargeLine,
        DotPayloadDistribution.SmallLine)]
    public DotPayloadDistribution Distribution { get; set; }

    /// <summary>
    /// Builds the selected payload and allocates the exact destination buffer required by the transform.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _source = BuildPayload(Distribution, PayloadSize);
        int required = TransitDotStuffing.GetRequiredDestinationLength(_source, appendTrailingCrlfWhenMissingLf: true, out _);
        _destination = GC.AllocateUninitializedArray<byte>(required);
    }

    /// <summary>
    /// Measures the baseline byte-at-a-time dot-stuffing implementation.
    /// <returns>The number of bytes written to the destination buffer.</returns>
    /// </summary>
    [Benchmark(Baseline = true, Description = "Transform/BaselineByteLoop")]
    [BenchmarkCategory("Transform")]
    public int Transform_BaselineByteLoop()
    {
        bool ok = TransitDotStuffing.TryDotStuff(
            _source,
            _destination,
            out TransitDotStuffTransformResult result,
            TransitDotStuffingAlgorithm.BaselineByteLoop,
            appendTrailingCrlfWhenMissingLf: true);
        if (!ok)
        {
            throw new InvalidOperationException("Dot-stuff baseline transform failed");
        }

        return result.BytesWritten;
    }

    /// <summary>
    /// Measures the line-oriented single-pass dot-stuffing implementation.
    /// <returns>The number of bytes written to the destination buffer.</returns>
    /// </summary>
    [Benchmark(Description = "Transform/BulkSinglePass")]
    [BenchmarkCategory("Transform")]
    public int Transform_BulkSinglePass()
    {
        bool ok = TransitDotStuffing.TryDotStuff(
            _source,
            _destination,
            out TransitDotStuffTransformResult result,
            TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass,
            appendTrailingCrlfWhenMissingLf: true);
        if (!ok)
        {
            throw new InvalidOperationException("Dot-stuff single-pass transform failed");
        }

        return result.BytesWritten;
    }

    /// <summary>
    /// Measures the line-oriented two-pass dot-stuffing implementation.
    /// <returns>The number of bytes written to the destination buffer.</returns>
    /// </summary>
    [Benchmark(Description = "Transform/BulkTwoPass")]
    [BenchmarkCategory("Transform")]
    public int Transform_BulkTwoPass()
    {
        bool ok = TransitDotStuffing.TryDotStuff(
            _source,
            _destination,
            out TransitDotStuffTransformResult result,
            TransitDotStuffingAlgorithm.BulkLineOrientedTwoPass,
            appendTrailingCrlfWhenMissingLf: true);
        if (!ok)
        {
            throw new InvalidOperationException("Dot-stuff two-pass transform failed");
        }

        return result.BytesWritten;
    }

    /// <summary>
    /// Measures baseline dot-stuffing when writing through a <see cref="PipeWriter"/>.
    /// <returns>The number of bytes written to the pipe-backed stream.</returns>
    /// </summary>
    [Benchmark(Baseline = true, Description = "PipeWriter/BaselineByteLoop")]
    [BenchmarkCategory("PipeWriter")]
    public int PipeWriter_BaselineByteLoop()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BaselineByteLoop);
    }

    /// <summary>
    /// Measures single-pass dot-stuffing when writing through a <see cref="PipeWriter"/>.
    /// <returns>The number of bytes written to the pipe-backed stream.</returns>
    /// </summary>
    [Benchmark(Description = "PipeWriter/BulkSinglePass")]
    [BenchmarkCategory("PipeWriter")]
    public int PipeWriter_BulkSinglePass()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass);
    }

    /// <summary>
    /// Measures two-pass dot-stuffing when writing through a <see cref="PipeWriter"/>.
    /// <returns>The number of bytes written to the pipe-backed stream.</returns>
    /// </summary>
    [Benchmark(Description = "PipeWriter/BulkTwoPass")]
    [BenchmarkCategory("PipeWriter")]
    public int PipeWriter_BulkTwoPass()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BulkLineOrientedTwoPass);
    }

    /// <summary>
    /// Writes ipeWriter.

    /// </summary>
    private int WriteWithPipeWriter(TransitDotStuffingAlgorithm algorithm)
    {
        int required = TransitDotStuffing.GetRequiredDestinationLength(_source, appendTrailingCrlfWhenMissingLf: true, out _);
        using MemoryStream stream = new(capacity: required + 64);
        PipeWriter writer = PipeWriter.Create(stream);
        Span<byte> destination = writer.GetSpan(required)[..required];

        bool ok = TransitDotStuffing.TryDotStuff(_source, destination, out TransitDotStuffTransformResult result, algorithm, appendTrailingCrlfWhenMissingLf: true);
        if (!ok)
        {
            throw new InvalidOperationException("PipeWriter transform failed");
        }

        writer.Advance(result.BytesWritten);
        ValueTask<FlushResult> flush = writer.FlushAsync();
        if (!flush.IsCompletedSuccessfully)
        {
            _ = flush.AsTask().GetAwaiter().GetResult();
        }

        return result.BytesWritten;
    }

    /// <summary>
    /// Builds ad.

    /// </summary>
    private static byte[] BuildPayload(DotPayloadDistribution distribution, int size)
    {
        return distribution switch
        {
            DotPayloadDistribution.NormalNoDot => BuildNormalPayload(size),
            DotPayloadDistribution.DotHeavy => BuildDotHeavyPayload(size),
            DotPayloadDistribution.Mixed => BuildMixedPayload(size),
            DotPayloadDistribution.LargeLine => BuildLargeLinePayload(size),
            DotPayloadDistribution.SmallLine => BuildSmallLinePayload(size),
            _ => BuildNormalPayload(size),
        };
    }

    /// <summary>
    /// Builds lPayload.

    /// </summary>
    private static byte[] BuildNormalPayload(int size)
    {
        byte[] data = GC.AllocateUninitializedArray<byte>(size);
        int index = 0;
        int value = 1;

        while (index < size)
        {
            int lineLength = Math.Min(170, size - index);
            for (int i = 0; i < lineLength && index < size; i++)
            {
                byte b = (byte)(value++ % 251);
                if (b == (byte)'\n')
                {
                    b = (byte)'A';
                }

                if (i == 0 && b == (byte)'.')
                {
                    b = (byte)'B';
                }

                data[index++] = b;
            }

            if (index < size)
            {
                data[index++] = (byte)'\n';
            }
        }

        data[^1] = (byte)'\n';
        return data;
    }

    /// <summary>
    /// Builds avyPayload.

    /// </summary>
    private static byte[] BuildDotHeavyPayload(int size)
    {
        byte[] data = GC.AllocateUninitializedArray<byte>(size);
        int index = 0;
        while (index < size)
        {
            int lineLength = Math.Min(40, size - index);
            for (int i = 0; i < lineLength && index < size; i++)
            {
                data[index++] = i == 0 ? (byte)'.' : (byte)'x';
            }

            if (index < size)
            {
                data[index++] = (byte)'\n';
            }
        }

        data[^1] = (byte)'\n';
        return data;
    }

    /// <summary>
    /// Builds Payload.

    /// </summary>
    private static byte[] BuildMixedPayload(int size)
    {
        byte[] data = GC.AllocateUninitializedArray<byte>(size);
        Random random = new(12345);
        int index = 0;
        int line = 0;

        while (index < size)
        {
            int lineLength = Math.Min(size - index, 80 + random.Next(160));
            bool dotStart = (line % 13) == 0;

            for (int i = 0; i < lineLength && index < size; i++)
            {
                if (i == 0 && dotStart)
                {
                    data[index++] = (byte)'.';
                    continue;
                }

                int v = random.Next(1, 255);
                byte b = v == (byte)'\n' ? (byte)'M' : (byte)v;
                data[index++] = b;
            }

            if (index < size)
            {
                data[index++] = (byte)'\n';
            }

            line++;
        }

        data[^1] = (byte)'\n';
        return data;
    }

    /// <summary>
    /// Builds LinePayload.

    /// </summary>
    private static byte[] BuildLargeLinePayload(int size)
    {
        byte[] data = GC.AllocateUninitializedArray<byte>(size);
        int index = 0;

        while (index < size)
        {
            int lineLength = Math.Min(8192, size - index);
            for (int i = 0; i < lineLength && index < size; i++)
            {
                data[index++] = (byte)('A' + (i % 23));
            }

            if (index < size)
            {
                data[index++] = (byte)'\n';
            }
        }

        data[^1] = (byte)'\n';
        return data;
    }

    /// <summary>
    /// Builds LinePayload.

    /// </summary>
    private static byte[] BuildSmallLinePayload(int size)
    {
        byte[] data = GC.AllocateUninitializedArray<byte>(size);
        int index = 0;
        int line = 0;

        while (index < size)
        {
            bool dotStart = (line & 3) == 0;
            if (index < size)
            {
                data[index++] = dotStart ? (byte)'.' : (byte)'a';
            }

            if (index < size)
            {
                data[index++] = (byte)'b';
            }

            if (index < size)
            {
                data[index++] = (byte)'\n';
            }

            line++;
        }

        data[^1] = (byte)'\n';
        return data;
    }
}

/// <summary>
/// Selects the synthetic line and dot distribution used by dot-stuffing benchmarks.
/// </summary>
public enum DotPayloadDistribution
{
    /// <summary>Payload containing no dot-prefixed lines.</summary>
    NormalNoDot = 0,
    /// <summary>Payload containing predominantly dot-prefixed lines.</summary>
    DotHeavy = 1,
    /// <summary>Payload containing a mixed distribution of line types.</summary>
    Mixed = 2,
    /// <summary>Payload containing unusually large lines.</summary>
    LargeLine = 3,
    /// <summary>Payload containing many short lines.</summary>
    SmallLine = 4,
}
