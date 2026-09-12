// <copyright file="MeasurementCohortLedgerContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement cohort and window accounting with an independent event ledger.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Verifies benchmark cohort and throughput accounting against an independent event ledger.
    /// </summary>
    public sealed class MeasurementCohortLedgerContractTests
    {
        [Fact]
        public void DelayedAck_CompletesAfterMeasurementBoundary_IsExcludedFromWindowAcceptedThroughput()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10, measurementArticleCount: 1);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);

            string messageId = "<delayed-ack@benchmark.usenet.ninja>";
            long offeredTick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, messageId, 1024, offeredTick);
            EmitAdmitted(metrics, ledger, messageId, 1024, offeredTick + 1);

            long measurementEndTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, measurementEndTick);

            EmitTerminal(metrics, ledger, messageId, 1024, TransitPublishStatus.Accepted, offeredTick + 2, measurementEndTick + 1);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(measurementEndTick);

            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
            Assert.Equal(0, result.AcceptedWithinWindowBytes);
            Assert.Equal(summary.AcceptedPostMeasurementBytes, result.AcceptedPostMeasurementBytes);
            Assert.Equal(1, result.AcceptedPostMeasurementArticles);
        }

        [Fact]
        public void AllRejectedTraffic_ReportsZeroAcceptedThroughput()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            for (int i = 0; i < 3; i++)
            {
                string messageId = $"<reject-{i}@benchmark.usenet.ninja>";
                long tick = Stopwatch.GetTimestamp();
                EmitOffered(metrics, ledger, messageId, 1024, tick);
                EmitAdmitted(metrics, ledger, messageId, 1024, tick + 1);
                EmitTerminal(metrics, ledger, messageId, 1024, TransitPublishStatus.Rejected, tick + 2, tick + 3);
            }

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(0, result.AcceptedWithinWindowArticles);
            Assert.Equal(0, result.AcceptedWithinWindowBytes);
            Assert.Equal(0d, result.AcceptedGbps);
            Assert.Equal(summary.RejectedCount, result.RejectedArticles);
        }

        [Fact]
        public void WarmupWork_IsExcludedFromMeasuredCohort()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            ledger.RecordWarmup("<warmup-1@benchmark.usenet.ninja>", 1024, Stopwatch.GetTimestamp());
            ledger.RecordWarmup("<warmup-2@benchmark.usenet.ninja>", 1024, Stopwatch.GetTimestamp());

            string measured = "<measured-1@benchmark.usenet.ninja>";
            long tick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, measured, 1024, tick);
            EmitAdmitted(metrics, ledger, measured, 1024, tick + 1);
            EmitTerminal(metrics, ledger, measured, 1024, TransitPublishStatus.Accepted, tick + 2, tick + 3);

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(1, summary.OfferedCount);
            Assert.Equal(3, ledger.TotalRecordedArticles);
            Assert.Equal(summary.OfferedCount, result.OfferedArticles);
            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
        }

        [Fact]
        public void FixedCountAdmission_DrainCompletionsRemainOutsideWindowThroughput()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10, measurementArticleCount: 4);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            (string Id, long Tick)[] admitted = new (string, long)[4];
            for (int i = 0; i < admitted.Length; i++)
            {
                admitted[i] = ($"<fixed-{i}@benchmark.usenet.ninja>", Stopwatch.GetTimestamp());
                EmitOffered(metrics, ledger, admitted[i].Id, 1024, admitted[i].Tick);
                EmitAdmitted(metrics, ledger, admitted[i].Id, 1024, admitted[i].Tick + 1);
            }

            EmitTerminal(metrics, ledger, admitted[0].Id, 1024, TransitPublishStatus.Accepted, admitted[0].Tick + 2, admitted[0].Tick + 3);
            EmitTerminal(metrics, ledger, admitted[1].Id, 1024, TransitPublishStatus.Accepted, admitted[1].Tick + 2, admitted[1].Tick + 3);

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            EmitTerminal(metrics, ledger, admitted[2].Id, 1024, TransitPublishStatus.Accepted, admitted[2].Tick + 2, boundaryTick + 1);
            EmitTerminal(metrics, ledger, admitted[3].Id, 1024, TransitPublishStatus.Accepted, admitted[3].Tick + 2, boundaryTick + 2);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(4, result.AdmittedArticles);
            Assert.Equal(summary.CompletedWithinWindowCount, result.CompletedWithinWindowArticles);
            Assert.Equal(summary.CompletedPostMeasurementCount, result.CompletedPostMeasurementArticles);
            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
            Assert.Equal(summary.AcceptedPostMeasurementBytes, result.AcceptedPostMeasurementBytes);
        }

        [Fact]
        public void OfferedAndAdmittedRemainDistinctUnderBackpressureMismatch()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            for (int i = 0; i < 5; i++)
            {
                string id = $"<offered-{i}@benchmark.usenet.ninja>";
                long tick = Stopwatch.GetTimestamp();
                EmitOffered(metrics, ledger, id, 1024, tick);

                if (i < 3)
                {
                    EmitAdmitted(metrics, ledger, id, 1024, tick + 1);
                    EmitTerminal(metrics, ledger, id, 1024, TransitPublishStatus.Accepted, tick + 2, tick + 3);
                }
            }

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(summary.OfferedWithinWindowCount, result.OfferedWithinWindowArticles);
            Assert.Equal(summary.AdmittedWithinWindowCount, result.AdmittedWithinWindowArticles);
            Assert.NotEqual(result.OfferedWithinWindowArticles, result.AdmittedWithinWindowArticles);
        }

        [Fact]
        public void AcceptedAndAmbiguousRemainSeparate()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            long acceptedTick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<accepted@benchmark.usenet.ninja>", 1024, acceptedTick);
            EmitAdmitted(metrics, ledger, "<accepted@benchmark.usenet.ninja>", 1024, acceptedTick + 1);
            EmitTerminal(metrics, ledger, "<accepted@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, acceptedTick + 2, acceptedTick + 3);

            long ambiguousTick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<ambiguous@benchmark.usenet.ninja>", 1024, ambiguousTick);
            EmitAdmitted(metrics, ledger, "<ambiguous@benchmark.usenet.ninja>", 1024, ambiguousTick + 1);
            EmitTerminal(metrics, ledger, "<ambiguous@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Ambiguous, ambiguousTick + 2, ambiguousTick + 3);

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(summary.AcceptedCount, result.AcceptedArticles);
            Assert.Equal(summary.AmbiguousCount, result.AmbiguousArticles);
            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
        }

        [Fact]
        public void DurationBoundary_IsDeterministicAndCannotRetroactivelyChangeDenominator()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);

            long beforeBoundaryTick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<before@benchmark.usenet.ninja>", 1024, beforeBoundaryTick);
            EmitAdmitted(metrics, ledger, "<before@benchmark.usenet.ninja>", 1024, beforeBoundaryTick + 1);
            EmitTerminal(metrics, ledger, "<before@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, beforeBoundaryTick + 2, beforeBoundaryTick + 3);

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            long afterBoundaryTick = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<after@benchmark.usenet.ninja>", 1024, afterBoundaryTick);
            EmitAdmitted(metrics, ledger, "<after@benchmark.usenet.ninja>", 1024, afterBoundaryTick + 1);
            EmitTerminal(metrics, ledger, "<after@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, afterBoundaryTick + 2, boundaryTick + 1);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(2, result.AcceptedArticles);
            Assert.Equal(1, result.AcceptedWithinWindowArticles);
            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
            Assert.Equal(summary.AcceptedPostMeasurementBytes, result.AcceptedPostMeasurementBytes);
        }

        [Fact]
        public void MutationGuard_PostMeasurementAcceptedBytesCannotInflateWindowThroughput()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            long first = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<m1@benchmark.usenet.ninja>", 1024, first);
            EmitAdmitted(metrics, ledger, "<m1@benchmark.usenet.ninja>", 1024, first + 1);
            EmitTerminal(metrics, ledger, "<m1@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, first + 2, first + 3);

            long boundaryTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            long second = Stopwatch.GetTimestamp();
            EmitOffered(metrics, ledger, "<m2@benchmark.usenet.ninja>", 1024, second);
            EmitAdmitted(metrics, ledger, "<m2@benchmark.usenet.ninja>", 1024, second + 1);
            EmitTerminal(metrics, ledger, "<m2@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, second + 2, boundaryTick + 1);

            BenchmarkResult result = BuildResult(config, metrics);
            double wrongThroughput = result.AcceptedBytes * 8d / 1_000_000_000d / result.Boundary.MeasurementWindowDuration.TotalSeconds;
            Assert.NotEqual(wrongThroughput, result.AcceptedGbps);
        }

        [Fact]
        public void MutationGuard_DrainDurationCannotReplaceMeasurementWindowDenominator()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);

            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);
            long tick = startTick + 100;
            metrics.OnGenerated(1024, tick, TransitBenchmarkCore.ProducerTiming.FromRaw(loopTicks: 10, generationTicks: 5, blockedTicks: 5, otherActiveTicks: 0), 5);
            metrics.OnAdmitted(1024, tick + 1);
            metrics.OnPublishResult(new TransitPublishResult("<denominator@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", tick + 2, tick + 3, tick + 4, tick + 5, tick + 6, tick + 7, tick + 8, tick + 9), 1024, tick + 1, tick + 2, tick + 9, 1, 0, completionTickOverride: tick + 9);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, tick + 10);

            BenchmarkResult result = BuildResult(config, metrics);
            double wrongThroughput = result.AcceptedWithinWindowBytes * 8d / 1_000_000_000d / Math.Max(0.000001d, result.Boundary.EndToEndDuration.TotalSeconds);
            Assert.NotEqual(wrongThroughput, result.AcceptedGbps);
        }

        [Fact]
        public void TerminalBoundaryClassification_UsesDeterministicTickRules_LessEqualGreater()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);
            IndependentMeasurementLedger ledger = new();

            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);

            long boundaryTick = startTick + 1_000;

            EmitOffered(metrics, ledger, "<less@benchmark.usenet.ninja>", 1024, startTick + 10);
            EmitAdmitted(metrics, ledger, "<less@benchmark.usenet.ninja>", 1024, startTick + 20);
            EmitTerminal(metrics, ledger, "<less@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, startTick + 30, boundaryTick - 1);

            EmitOffered(metrics, ledger, "<equal@benchmark.usenet.ninja>", 1024, startTick + 40);
            EmitAdmitted(metrics, ledger, "<equal@benchmark.usenet.ninja>", 1024, startTick + 50);
            EmitTerminal(metrics, ledger, "<equal@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, startTick + 60, boundaryTick);

            EmitOffered(metrics, ledger, "<greater@benchmark.usenet.ninja>", 1024, startTick + 70);
            EmitAdmitted(metrics, ledger, "<greater@benchmark.usenet.ninja>", 1024, startTick + 80);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);
            EmitTerminal(metrics, ledger, "<greater@benchmark.usenet.ninja>", 1024, TransitPublishStatus.Accepted, startTick + 90, boundaryTick + 1);

            BenchmarkResult result = BuildResult(config, metrics);
            LedgerSummary summary = ledger.Summarize(boundaryTick);

            Assert.Equal(summary.AcceptedWithinWindowBytes, result.AcceptedWithinWindowBytes);
            Assert.Equal(summary.AcceptedPostMeasurementBytes, result.AcceptedPostMeasurementBytes);
            Assert.Equal(2, result.AcceptedWithinWindowArticles);
            Assert.Equal(1, result.AcceptedPostMeasurementArticles);
        }

        private static BenchmarkResult BuildResult(TransitBenchmarkConfig config, MeasurementMetrics metrics)
        {
            MeasurementSnapshot snapshot = metrics.Snapshot();
            return BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                metrics,
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: metrics.GetCompletedPostMeasurementCount(),
                allocatedStartBytes: 0,
                enableForensicDiagnostics: false);
        }

        private static void EmitOffered(MeasurementMetrics metrics, IndependentMeasurementLedger ledger, string messageId, int bytes, long offeredTick)
        {
            metrics.OnGenerated(bytes, offeredTick, TransitBenchmarkCore.ProducerTiming.FromRaw(loopTicks: 10, generationTicks: 3, blockedTicks: 7, otherActiveTicks: 0), queueWaitTicks: 7);
            ledger.RecordOffered(messageId, bytes, offeredTick);
        }

        private static void EmitAdmitted(MeasurementMetrics metrics, IndependentMeasurementLedger ledger, string messageId, int bytes, long admittedTick)
        {
            metrics.OnAdmitted(bytes, admittedTick);
            ledger.RecordAdmitted(messageId, admittedTick);
        }

        private static void EmitTerminal(MeasurementMetrics metrics, IndependentMeasurementLedger ledger, string messageId, int bytes, TransitPublishStatus status, long publishStartTick, long terminalTick)
        {
            metrics.OnPublishResult(
                new TransitPublishResult(
                    MessageId: messageId,
                    Status: status,
                    ResponseCode: status == TransitPublishStatus.Accepted ? 239 : 400,
                    ResponseText: status.ToString(),
                    T0PublishAsyncEnterTick: publishStartTick,
                    T1DispatcherAssignedTick: publishStartTick + 1,
                    T2SocketWriteBeginTick: publishStartTick + 2,
                    T3SocketWriteEndTick: publishStartTick + 3,
                    T4ResponseAvailableTick: publishStartTick + 4,
                    T5ResponseParsedTick: publishStartTick + 5,
                    T6ResponseCorrelatedTick: publishStartTick + 6,
                    T7PublishAsyncCompleteTick: terminalTick),
                bytes,
                dequeuedTick: publishStartTick - 1,
                publishStartTick,
                publishEndTick: terminalTick,
                pendingAtSubmit: 1,
                pendingAtComplete: 0,
                completionTickOverride: terminalTick);

            ledger.RecordTerminal(messageId, status, terminalTick);
        }

        private sealed class IndependentMeasurementLedger
        {
            private readonly Dictionary<string, LedgerEntry> _entries = new(StringComparer.Ordinal);
            private int _warmupCount;

            internal int TotalRecordedArticles => _entries.Count + _warmupCount;

            internal void RecordWarmup(string messageId, int bytes, long offeredTick)
            {
                _warmupCount++;
                _ = messageId;
                _ = bytes;
                _ = offeredTick;
            }

            internal void RecordOffered(string messageId, int bytes, long offeredTick)
            {
                LedgerEntry entry = GetOrCreate(messageId);
                entry.Bytes = bytes;
                entry.OfferedTick = offeredTick;
            }

            internal void RecordAdmitted(string messageId, long admittedTick)
            {
                LedgerEntry entry = GetOrCreate(messageId);
                entry.AdmittedTick = admittedTick;
            }

            internal void RecordTerminal(string messageId, TransitPublishStatus status, long terminalTick)
            {
                LedgerEntry entry = GetOrCreate(messageId);
                entry.Status = status;
                entry.TerminalTick = terminalTick;
            }

            internal LedgerSummary Summarize(long measurementEndTick)
            {
                LedgerSummary summary = new();

                foreach ((_, LedgerEntry entry) in _entries)
                {
                    if (entry.OfferedTick > 0)
                    {
                        summary.OfferedCount++;
                        if (entry.OfferedTick <= measurementEndTick)
                        {
                            summary.OfferedWithinWindowCount++;
                        }
                    }

                    if (entry.AdmittedTick > 0)
                    {
                        summary.AdmittedCount++;
                        if (entry.AdmittedTick <= measurementEndTick)
                        {
                            summary.AdmittedWithinWindowCount++;
                        }
                    }

                    if (entry.TerminalTick > 0)
                    {
                        bool postMeasurement = entry.TerminalTick > measurementEndTick;
                        if (postMeasurement)
                        {
                            summary.CompletedPostMeasurementCount++;
                        }
                        else
                        {
                            summary.CompletedWithinWindowCount++;
                        }

                        switch (entry.Status)
                        {
                            case TransitPublishStatus.Accepted:
                                summary.AcceptedCount++;
                                if (postMeasurement)
                                {
                                    summary.AcceptedPostMeasurementBytes += entry.Bytes;
                                }
                                else
                                {
                                    summary.AcceptedWithinWindowBytes += entry.Bytes;
                                }

                                break;
                            case TransitPublishStatus.Rejected:
                                summary.RejectedCount++;
                                break;
                            case TransitPublishStatus.Ambiguous:
                            case TransitPublishStatus.Failed:
                            case TransitPublishStatus.Unavailable:
                            case TransitPublishStatus.Canceled:
                                summary.AmbiguousCount++;
                                break;
                        }
                    }
                }

                return summary;
            }

            private LedgerEntry GetOrCreate(string messageId)
            {
                if (!_entries.TryGetValue(messageId, out LedgerEntry? entry))
                {
                    entry = new LedgerEntry();
                    _entries[messageId] = entry;
                }

                return entry;
            }
        }

        private sealed class LedgerEntry
        {
            internal int Bytes;
            internal long OfferedTick;
            internal long AdmittedTick;
            internal TransitPublishStatus Status;
            internal long TerminalTick;
        }

        private sealed class LedgerSummary
        {
            internal int OfferedCount;
            internal int OfferedWithinWindowCount;
            internal int AdmittedCount;
            internal int AdmittedWithinWindowCount;
            internal int AcceptedCount;
            internal int RejectedCount;
            internal int AmbiguousCount;
            internal int CompletedWithinWindowCount;
            internal int CompletedPostMeasurementCount;
            internal long AcceptedWithinWindowBytes;
            internal long AcceptedPostMeasurementBytes;
        }
    }
}
