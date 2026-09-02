// <copyright file="TopologyReporter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Reporting/TopologyReporter: formats benchmark measurements and topology details for operators and artifacts.

using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the topology Reporter class used by the benchmark or regression gate.
/// </summary>
internal static class TopologyReporter
{
    /// <summary>
    /// Implements the print ConnectionTopologyDiagnostics contract.
    /// </summary>
    internal static void PrintConnectionTopologyDiagnostics(TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics)
    {
        Console.WriteLine($"Configured pool size: {diagnostics.ConfiguredConnectionPoolSize}");
        Console.WriteLine($"Configured per-connection pipeline depth: {diagnostics.ConfiguredPerConnectionPipelineDepth}");
        Console.WriteLine($"Global reconnect count: {diagnostics.TotalReconnects}");

        int uniqueConnectionCount = diagnostics.Connections.Select(static x => x.Snapshot.ConnectionId).Distinct(StringComparer.Ordinal).Count();
        long totalSocketOpens = diagnostics.Connections.Sum(static x => x.Snapshot.SocketOpenCount);
        int readyConnectionCount = diagnostics.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0);
        int activeConnectionCount = diagnostics.Slots.Count(static x => x.TotalSubmissionsRouted > 0);

        Console.WriteLine($"Unique TransitConnection instances observed: {uniqueConnectionCount}");
        Console.WriteLine($"Total physical socket opens observed: {totalSocketOpens}");
        Console.WriteLine($"Connections reaching READY at least once: {readyConnectionCount}");
        Console.WriteLine($"Pool slots that carried submissions: {activeConnectionCount}/{diagnostics.ConfiguredConnectionPoolSize}");

        Console.WriteLine();
        Console.WriteLine("Per-slot participation:");
        foreach (TransitPublisher.ConnectionSlotSnapshot slot in diagnostics.Slots.OrderBy(static x => x.SlotIndex))
        {
            Console.WriteLine($"  Slot {slot.SlotIndex}: HasCurrent={slot.HasCurrentConnection}, CurrentConnectionId={slot.CurrentConnectionId ?? "(none)"}, RoutedSubmissions={slot.TotalSubmissionsRouted}, Reconnects={slot.Reconnects}, CreatedConnections={slot.CreatedConnections}, MaxObservedInFlightDepth={slot.MaxObservedInFlightDepth}, WaitedForChannelReadability={slot.WaitedForChannelReadabilityCount}, WaitedForCompletionWhileFull={slot.WaitedForCompletionWhilePipelineFullCount}, FirstReachedConfiguredDepthTick={slot.FirstReachedConfiguredDepthTick}");
        }

        Console.WriteLine();
        Console.WriteLine("Per-connection diagnostics:");
        foreach (TransitPublisher.ConnectionDiagnosticsEntry entry in diagnostics.Connections
                     .OrderBy(static x => x.SlotIndex)
                     .ThenBy(static x => x.Snapshot.ConnectionId, StringComparer.Ordinal))
        {
            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = entry.Snapshot;
            Console.WriteLine($"  Slot {entry.SlotIndex}, ConnectionId={snapshot.ConnectionId}, Host={snapshot.Host}:{snapshot.Port}, State={snapshot.CurrentState}, TLS={snapshot.IsTlsActive}");
            Console.WriteLine($"    Endpoints: Local={snapshot.LocalEndpoint ?? "(unavailable)"}, Remote={snapshot.RemoteEndpoint ?? "(unavailable)"}");
            Console.WriteLine($"    SocketOpens={snapshot.SocketOpenCount}, ReadyTransitions={snapshot.ReadyTransitionCount}, MaxInFlight={snapshot.MaxConcurrentSubmissions}, CurrentInFlight={snapshot.CurrentConcurrentSubmissions}");
            Console.WriteLine($"    Submissions: Started={snapshot.SubmissionsStarted}, Accepted={snapshot.SubmissionsAccepted}, Rejected={snapshot.SubmissionsRejected}, Ambiguous={snapshot.SubmissionsAmbiguous}, Unavailable={snapshot.SubmissionsUnavailable}, Failed={snapshot.SubmissionsFailed}");

            TransitConnection.PipeliningDiagnosticSummary diagnosticSummary = snapshot.DiagnosticsSummary;
            Console.WriteLine($"    Pipelining diagnostics: MaxPendingDepth={diagnosticSummary.MaxPendingDepth}, MaxWriteQueueDepth={diagnosticSummary.MaxWriteQueueDepth}, MaxWriterBatchSize={diagnosticSummary.MaxWriterBatchSize}, AvgBatchSize={diagnosticSummary.AverageWriterBatchSize:F2}, P50={diagnosticSummary.P50WriterBatchSize:F0}, P95={diagnosticSummary.P95WriterBatchSize:F0}, P99={diagnosticSummary.P99WriterBatchSize:F0}, NumberOfBatches={diagnosticSummary.NumberOfBatches}");
            Console.WriteLine($"    Batch histogram: {diagnosticSummary.BatchSizeHistogram}");
            Console.WriteLine($"    Coalescing wait (us): Avg={diagnosticSummary.AverageCoalescingWaitMicroseconds:F2}, P50={diagnosticSummary.P50CoalescingWaitMicroseconds:F2}, P95={diagnosticSummary.P95CoalescingWaitMicroseconds:F2}, P99={diagnosticSummary.P99CoalescingWaitMicroseconds:F2}");
            Console.WriteLine($"    MaxLaterTakethisBeforeResponse={diagnosticSummary.MaxLogicalOutstandingAheadAtResponse}, CapturedOperationCount={diagnosticSummary.CapturedOperationCount}, SampledOperationCount={diagnosticSummary.SampledOperationCount}");

            int sampleCount = Math.Min(snapshot.DiagnosticSampleRecords.Length, 1000);
            if (sampleCount > 0)
            {
                Console.WriteLine($"    Diagnostic sample records shown: {sampleCount}");
                for (int i = 0; i < sampleCount; i++)
                {
                    TransitConnection.DiagnosticOperationRecord sample = snapshot.DiagnosticSampleRecords[i];
                    Console.WriteLine($"      MessageId={sample.MessageId}; T0={sample.T0SubmitEnterTick}; T0SubmitTakethis={sample.T0SubmitTakethisEnterTick}; T1={sample.T1PendingRegisteredTick}; T2EnqueueStart={sample.T2WriteIntentEnqueueStartTick}; T2={sample.T2WriteIntentEnqueuedTick}; T2BeforeAwait={sample.T2BeforeCompletionAwaitTick}; T3={sample.T3WriterDequeuedTick}; T4={sample.T4AssignedToBatchTick}; T5={sample.T5FrameStageBeginTick}; T6={sample.T6FrameStageEndTick}; T7={sample.T7BatchFlushBeginTick}; T8={sample.T8BatchFlushEndTick}; T9={sample.T9ResponseCorrelatedTick}; T10={sample.T10SubmitCompletionTick}; PendingT1={sample.PendingDepthAtT1}; PendingT2={sample.PendingDepthAtT2}; PendingT3={sample.PendingDepthAtT3}; PendingT4={sample.PendingDepthAtT4}; PendingT9={sample.PendingDepthAtT9}; QueueT2={sample.QueueDepthAtT2}; QueueT3={sample.QueueDepthAtT3}; QueueBatchStart={sample.QueueDepthAtBatchStart}; BatchDequeued={sample.BatchDequeuedCount}; QueueT9={sample.QueueDepthAtT9}; BatchId={sample.BatchId}; BatchPosition={sample.BatchPosition}; BatchSize={sample.BatchSize}; SendSequence={sample.SendSequence}; LaterTakethisBefore239={sample.LogicalOutstandingAheadAtResponse}");
                }
            }
        }

        int submissionTraceCount = diagnostics.SubmissionTraceRecords.Length;
        Console.WriteLine();
        Console.WriteLine($"Submission pump trace records: {submissionTraceCount}");
        if (submissionTraceCount > 0)
        {
            int submissionTraceSampleCount = Math.Min(submissionTraceCount, 1000);
            Console.WriteLine($"Submission pump trace sample shown: {submissionTraceSampleCount}");
            for (int i = 0; i < submissionTraceSampleCount; i++)
            {
                TransitPublisher.SubmissionTraceRecord trace = diagnostics.SubmissionTraceRecords[i];
                Console.WriteLine($"  MessageId={trace.MessageId}; PumpReadTick={trace.RemovedFromSubmissionChannelTick}; PublishInvokeTick={trace.PublishToConnectionInvokedTick}; InFlightBeforeAdd={trace.InFlightCountBeforeAdd}; InFlightAfterAdd={trace.InFlightCountAfterAdd}; WriteIntentQueueDepthAtRead={trace.WriteIntentQueueDepthAtPumpRead}");
            }
        }

        int publishTraceCount = diagnostics.PublishToConnectionTraceRecords.Length;
        Console.WriteLine();
        Console.WriteLine($"PublishToConnectionWithReconnect trace records: {publishTraceCount}");
        if (publishTraceCount > 0)
        {
            int publishTraceSampleCount = Math.Min(publishTraceCount, 1000);
            Console.WriteLine($"PublishToConnectionWithReconnect trace sample shown: {publishTraceSampleCount}");
            for (int i = 0; i < publishTraceSampleCount; i++)
            {
                TransitPublisher.PublishToConnectionTraceRecord trace = diagnostics.PublishToConnectionTraceRecords[i];
                Console.WriteLine($"  MessageId={trace.MessageId}; Slot={trace.SlotIndex}; MethodEntryTick={trace.MethodEntryTick}; ConnectionId={trace.SelectedConnectionId ?? "(null)"}; BeforeSubmitTakethisTick={trace.BeforeSubmitTakethisTick}; AfterSubmitTakethisTick={trace.AfterSubmitTakethisTick}");
            }
        }
    }
}
