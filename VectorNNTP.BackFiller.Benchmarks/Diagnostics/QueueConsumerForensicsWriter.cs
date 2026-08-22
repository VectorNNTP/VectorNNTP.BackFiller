using System.Globalization;
using System.Text;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Exports the dispatch consumer queue-read forensic report to the JSON and text artifacts.
/// </summary>
internal static class QueueConsumerForensicsWriter
{
    internal const string JsonFileName = "queue-consumer-callstacks.json";
    internal const string TextFileName = "queue-consumer-callstacks.txt";

    /// <summary>
    /// Writes both forensic artifacts into the supplied directory.
    /// </summary>
    /// <param name="report">The report to export.</param>
    /// <param name="directory">Target directory.</param>
    /// <returns>The written JSON and text artifact paths.</returns>
    internal static (string JsonPath, string TextPath) Write(QueueConsumerForensicsReport report, string directory)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);

        Directory.CreateDirectory(directory);

        string jsonPath = Path.Combine(directory, JsonFileName);
        string textPath = Path.Combine(directory, TextFileName);

        File.WriteAllText(jsonPath, JsonArtifactWriter.Serialize(report));
        File.WriteAllText(textPath, RenderText(report));

        return (jsonPath, textPath);
    }

    /// <summary>
    /// Renders the human readable forensic artifact.
    /// </summary>
    /// <param name="report">The report to render.</param>
    /// <returns>The rendered text.</returns>
    internal static string RenderText(QueueConsumerForensicsReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        StringBuilder builder = new();
        builder.AppendLine("=== DISPATCH CONSUMER QUEUE-READ FORENSICS ===");
        builder.AppendLine(Invariant($"Generated (UTC):            {report.GeneratedUtc:O}"));
        builder.AppendLine(Invariant($"Observed window (ms):       {report.ObservedWindowMilliseconds:F1}"));
        builder.AppendLine(Invariant($"Dispatch consumers:         {report.ConsumerCount}"));
        builder.AppendLine(Invariant($"Long-wait threshold (ms):   {report.LongWaitThresholdMilliseconds}"));
        builder.AppendLine();

        builder.AppendLine("--- WAIT / TRYREAD TOTALS ---");
        builder.AppendLine(Invariant($"Wait episodes:                       {report.WaitEpisodeCount}"));
        builder.AppendLine(Invariant($"  completed synchronously (no park): {report.WaitEpisodesCompletedSynchronously}"));
        builder.AppendLine(Invariant($"  parked (awaited):                  {report.WaitEpisodesParked}"));
        builder.AppendLine(Invariant($"  longer than threshold:             {report.LongWaitEpisodeCount}"));
        builder.AppendLine(Invariant($"Max simultaneous waiters:            {report.MaxConcurrentWaiters}"));
        builder.AppendLine(Invariant($"Max simultaneous TryRead calls:      {report.MaxConcurrentTryReads}"));
        builder.AppendLine(Invariant($"TryRead attempts:                    {report.TryReadAttemptCount}"));
        builder.AppendLine(Invariant($"TryRead successes:                   {report.TryReadSuccessCount}"));
        builder.AppendLine(Invariant($"TryRead failures:                    {report.TryReadFailureCount}"));
        builder.AppendLine(Invariant($"Producer enqueues:                   {report.EnqueueCount}"));
        builder.AppendLine(Invariant($"Consumer resumed on another thread:  {report.ThreadHopsAcrossWait}"));
        builder.AppendLine();

        builder.AppendLine("--- FAILED TRYREAD RECONCILIATION AGAINST CurrentQueuedCount ---");
        builder.AppendLine(Invariant($"A  count == 0 before TryRead:              {report.TryReadFailuresClassA}"));
        builder.AppendLine(Invariant($"B  count  > 0 before TryRead (unchanged):  {report.TryReadFailuresClassB}"));
        builder.AppendLine(Invariant($"C  count changed during observation:       {report.TryReadFailuresClassC}"));
        builder.AppendLine(Invariant($"D  undeterminable:                         {report.TryReadFailuresClassD}"));
        if (report.TryReadFailuresClassD > 0)
        {
            builder.AppendLine("   Class D means a negative CurrentQueuedCount was observed around the failure: the counter is maintained independently of the Channel and transiently drifts below zero.");
        }
        builder.AppendLine(report.AnyTryReadFailureWithPositiveDepth
            ? "TryRead == false DID occur while CurrentQueuedCount > 0 (see class B records below)."
            : "TryRead == false NEVER occurred while CurrentQueuedCount > 0 and unchanged across the observation.");
        builder.AppendLine();

        builder.AppendLine("--- INTERVAL BREAKDOWN (microseconds, long-wait episodes) ---");
        builder.AppendLine("interval  samples        p50        p95        p99        max  description");
        foreach (IntervalStatistics interval in report.Intervals)
        {
            builder.AppendLine(Invariant(
                $"{interval.Interval,-8}  {interval.SampleCount,7}  {interval.P50Microseconds,9:F1}  {interval.P95Microseconds,9:F1}  {interval.P99Microseconds,9:F1}  {interval.MaxMicroseconds,9:F1}  {interval.Description}"));
        }

        builder.AppendLine();
        builder.AppendLine("--- CONSUMER STATE CENSUS ---");
        builder.AppendLine("elapsed_ms  created  waiting  wait_returned  try_reading  processing  exited");
        foreach (ConsumerStateCensus census in report.StateCensuses)
        {
            builder.AppendLine(Invariant(
                $"{census.ElapsedMillisecondsSinceStart,10:F1}  {census.Created,7}  {census.WaitingToRead,7}  {census.WaitReturned,13}  {census.TryReading,11}  {census.ProcessingArticle,10}  {census.Exited,6}"));
        }

        builder.AppendLine();
        builder.AppendLine("--- OUTSTANDING WORK OWNERSHIP ---");
        builder.AppendLine(Invariant($"Channel queued (accounting):  {report.Ownership.ChannelQueuedByAccounting} articles / {report.Ownership.ChannelQueuedBytesByAccounting} bytes"));
        builder.AppendLine(Invariant($"Consumer-owned:               {report.Ownership.ConsumerOwnedArticles} articles"));
        builder.AppendLine(Invariant($"Transport in-flight:          {report.Ownership.TransportInFlightArticles} articles"));
        builder.AppendLine(Invariant($"Total outstanding work:       {report.Ownership.TotalOutstandingWork} articles"));
        builder.AppendLine(report.Ownership.Note);

        builder.AppendLine();
        builder.AppendLine(Invariant($"--- FIRST {report.FirstLongWaits.Count} WAITS LONGER THAN {report.LongWaitThresholdMilliseconds} ms ---"));
        foreach (LongWaitRecord wait in report.FirstLongWaits)
        {
            AppendLongWait(builder, wait);
        }

        builder.AppendLine();
        builder.AppendLine("--- CLASS B FAILED TRYREAD RECORDS (depth > 0 before failure) ---");
        bool anyClassB = false;
        foreach (TryReadFailureRecord failure in report.TryReadFailures)
        {
            if (!string.Equals(failure.Classification, nameof(TryReadFailureClass.CountPositiveBefore), StringComparison.Ordinal))
            {
                continue;
            }

            anyClassB = true;
            builder.AppendLine(Invariant(
                $"t={failure.ElapsedMillisecondsSinceStart:F3}ms consumer={failure.ConsumerId} thread={failure.ManagedThreadId} task={FormatTaskId(failure.TaskId)} depthBefore={failure.QueueDepthBefore} depthAfter={failure.QueueDepthAfter} bytesBefore={failure.QueueBytesBefore} bytesAfter={failure.QueueBytesAfter} tryRead={failure.TryReadMicroseconds:F2}us"));
        }

        if (!anyClassB)
        {
            builder.AppendLine("(none)");
        }

        builder.AppendLine();
        builder.AppendLine("--- REPRESENTATIVE MANAGED STACKS ---");
        foreach (QueueConsumerStackSample sample in report.StackSamples)
        {
            builder.AppendLine(Invariant(
                $"[{sample.Phase}] waiterBucket={sample.WaiterBucket} consumer={sample.ConsumerId} thread={sample.ManagedThreadId} task={FormatTaskId(sample.TaskId)} t={sample.ElapsedMillisecondsSinceStart:F3}ms state={sample.ConsumerState} depth={sample.QueueDepth} bytes={sample.QueueBytes} waiters={sample.ConcurrentWaiters} tryReads={sample.ConcurrentTryReads}"));
            builder.AppendLine(Invariant(
                $"    syncContext={sample.SynchronizationContext} taskScheduler={sample.TaskScheduler} tpThreads={sample.ThreadPoolThreadCount} tpAvailWorkers={sample.ThreadPoolAvailableWorkerThreads} tpAvailIocp={sample.ThreadPoolAvailableCompletionPortThreads} tpPending={sample.ThreadPoolPendingWorkItemCount}"));
            builder.AppendLine(Indent(sample.StackTrace));
        }

        builder.AppendLine();
        builder.AppendLine("--- OBSERVABILITY NOTES ---");
        foreach (string note in report.ObservabilityNotes)
        {
            builder.AppendLine("* " + note);
        }

        return builder.ToString();
    }

    private static void AppendLongWait(StringBuilder builder, LongWaitRecord wait)
    {
        builder.AppendLine();
        builder.AppendLine(Invariant($"#{wait.Ordinal} consumer={wait.ConsumerId} waitStartUtc={wait.WaitStartUtc:O}"));
        builder.AppendLine(Invariant($"    threads: waitStart={wait.WaitStartThreadId} waitReturn={wait.WaitReturnThreadId} | tasks: waitStart={FormatTaskId(wait.WaitStartTaskId)} waitReturn={FormatTaskId(wait.WaitReturnTaskId)}"));
        builder.AppendLine(Invariant($"    WAIT_START       t={wait.WaitStartMs:F3}ms depth={wait.QueueDepthAtWaitStart} bytes={wait.QueueBytesAtWaitStart} waiters={wait.ConcurrentWaitersAtWaitStart} completedSynchronously={wait.WaitCompletedSynchronously}"));
        builder.AppendLine(Invariant($"    CHANNEL_WRITE_T0 t={FormatMs(wait.ChannelWriteStartMs)} (before WriteAsync)"));
        builder.AppendLine(Invariant($"    FIRST_ENQUEUE T1 t={FormatMs(wait.FirstEnqueueMs)} (WriteAsync returned, item readable) correlation={wait.EnqueueCorrelation}"));
        builder.AppendLine(Invariant($"    BATCH_ELIGIBLE   t={FormatMs(wait.BatchEligibleMs)} (accounting updated)"));
        builder.AppendLine(Invariant($"    WAIT_RETURN T2   t={wait.WaitReturnMs:F3}ms depth={wait.QueueDepthAtWaitReturn} bytes={wait.QueueBytesAtWaitReturn} waiters={wait.ConcurrentWaitersAtWaitReturn} result={wait.WaitResult}"));
        builder.AppendLine(Invariant($"    TRYREAD_START    t={wait.TryReadStartMs:F3}ms depth={wait.QueueDepthBeforeTryRead}"));
        builder.AppendLine(Invariant($"    TRYREAD_END      t={wait.TryReadEndMs:F3}ms depth={wait.QueueDepthAfterTryRead} result={wait.TryReadResult}"));
        builder.AppendLine(Invariant($"    A={FormatUs(wait.IntervalAWaitStartToFirstEnqueueUs)} B={FormatUs(wait.IntervalBFirstEnqueueToBatchEligibleUs)} C0={FormatUs(wait.IntervalC0ChannelWriteAsyncDurationUs)} C={FormatUs(wait.IntervalCBatchEligibleToWaitReturnUs)} D={FormatUs(wait.IntervalDWaitReturnToTryReadStartUs)} E={FormatUs(wait.IntervalETryReadDurationUs)} total={FormatUs(wait.TotalWaitUs)}"));
        builder.AppendLine(Invariant($"    threadPool@CHANNEL_WRITE: pendingWorkItems={wait.ThreadPoolPendingWorkItemsAtChannelWrite} consumersWaiting={wait.ConsumersWaitingAtChannelWrite}"));
        builder.AppendLine(Invariant($"    threadPool@WAIT_RETURN: threads={wait.ThreadPoolThreadCountAtWaitReturn} availableWorkers={wait.ThreadPoolAvailableWorkerThreadsAtWaitReturn} availableIocp={wait.ThreadPoolAvailableCompletionPortThreadsAtWaitReturn} pendingWorkItems={wait.ThreadPoolPendingWorkItemsAtWaitReturn}"));
        builder.AppendLine(Invariant($"    syncContext={wait.SynchronizationContextAtWaitReturn} taskScheduler={wait.TaskSchedulerAtWaitReturn}"));

        AppendOptionalStack(builder, "WAIT_START stack", wait.WaitStartStack);
        AppendOptionalStack(builder, "WAIT_RETURN stack", wait.WaitReturnStack);
        AppendOptionalStack(builder, "TRYREAD_START stack", wait.TryReadStartStack);
    }

    private static void AppendOptionalStack(StringBuilder builder, string title, string? stack)
    {
        if (string.IsNullOrWhiteSpace(stack))
        {
            return;
        }

        builder.AppendLine("    " + title + ":");
        builder.AppendLine(Indent(stack));
    }

    private static string Indent(string text)
    {
        StringBuilder builder = new();
        foreach (string line in text.Split('\n'))
        {
            string trimmed = line.TrimEnd('\r');
            if (trimmed.Length == 0)
            {
                continue;
            }

            builder.AppendLine("        " + trimmed);
        }

        return builder.ToString().TrimEnd('\r', '\n');
    }

    private static string FormatTaskId(int? taskId)
    {
        return taskId is null ? "(none)" : taskId.Value.ToString(CultureInfo.InvariantCulture);
    }

    private static string FormatMs(double milliseconds)
    {
        return double.IsNaN(milliseconds) ? "(unresolved)" : Invariant($"{milliseconds:F3}ms");
    }

    private static string FormatUs(double microseconds)
    {
        return double.IsNaN(microseconds) ? "(unresolved)" : Invariant($"{microseconds:F1}us");
    }

    private static string Invariant(FormattableString formattable)
    {
        return formattable.ToString(CultureInfo.InvariantCulture);
    }
}
