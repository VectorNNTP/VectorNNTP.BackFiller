// <copyright file="TransitDotStuffingBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// TransitDotStuffingBenchmarks: defines the benchmark entry point or scenario for controlled performance validation.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using System.Buffers;
using System.IO.Pipelines;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
/// <summary>
/// Represents the transit DotStuffingBenchmarks class used by this benchmark or regression-gate component.
/// </summary>
public class TransitDotStuffingBenchmarks
{
    /// <summary>
    /// Gets or sets the payload Size value used by this component.
    /// </summary>
    private const int PayloadSize = 2_097_152;

    /// <summary>
    /// Gets or sets the _source value used by this component.
    /// </summary>
    private byte[] _source = null!;
    /// <summary>
    /// Gets or sets the _destination value used by this component.
    /// </summary>
    private byte[] _destination = null!;

    [Params(
        DotPayloadDistribution.NormalNoDot,
        DotPayloadDistribution.DotHeavy,
        DotPayloadDistribution.Mixed,
        DotPayloadDistribution.LargeLine,
        DotPayloadDistribution.SmallLine)]
    /// <summary>
    /// Gets or sets the distribution value used by this component.
    /// </summary>
    public DotPayloadDistribution Distribution { get; set; }

    [GlobalSetup]
    /// <summary>
    /// Executes the setup operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public void Setup()
    {
        _source = BuildPayload(Distribution, PayloadSize);
        int required = TransitDotStuffing.GetRequiredDestinationLength(_source, appendTrailingCrlfWhenMissingLf: true, out _);
        _destination = GC.AllocateUninitializedArray<byte>(required);
    }

    [Benchmark(Baseline = true, Description = "Transform/BaselineByteLoop")]
    [BenchmarkCategory("Transform")]
    /// <summary>
    /// Executes the transform _BaselineByteLoop operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    [Benchmark(Description = "Transform/BulkSinglePass")]
    [BenchmarkCategory("Transform")]
    /// <summary>
    /// Executes the transform _BulkSinglePass operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    [Benchmark(Description = "Transform/BulkTwoPass")]
    [BenchmarkCategory("Transform")]
    /// <summary>
    /// Executes the transform _BulkTwoPass operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    [Benchmark(Baseline = true, Description = "PipeWriter/BaselineByteLoop")]
    [BenchmarkCategory("PipeWriter")]
    /// <summary>
    /// Executes the pipe Writer_BaselineByteLoop operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public int PipeWriter_BaselineByteLoop()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BaselineByteLoop);
    }

    [Benchmark(Description = "PipeWriter/BulkSinglePass")]
    [BenchmarkCategory("PipeWriter")]
    /// <summary>
    /// Executes the pipe Writer_BulkSinglePass operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public int PipeWriter_BulkSinglePass()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass);
    }

    [Benchmark(Description = "PipeWriter/BulkTwoPass")]
    [BenchmarkCategory("PipeWriter")]
    /// <summary>
    /// Executes the pipe Writer_BulkTwoPass operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public int PipeWriter_BulkTwoPass()
    {
        return WriteWithPipeWriter(TransitDotStuffingAlgorithm.BulkLineOrientedTwoPass);
    }

    /// <summary>
    /// Executes the write WithPipeWriter operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build Payload operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build NormalPayload operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build DotHeavyPayload operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build MixedPayload operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build LargeLinePayload operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the build SmallLinePayload operation while preserving the component's benchmark or test-harness contract.
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
/// Represents the dot PayloadDistribution enum used by this benchmark or regression-gate component.
/// </summary>
public enum DotPayloadDistribution
{
    NormalNoDot = 0,
    DotHeavy = 1,
    Mixed = 2,
    LargeLine = 3,
    SmallLine = 4,
}
