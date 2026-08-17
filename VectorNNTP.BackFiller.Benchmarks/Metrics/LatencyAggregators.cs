using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal readonly record struct ConnectionCounterState(
    string ConnectionId,
    TimeSpan Elapsed,
    long SubmissionsStarted,
    long Completed);

internal readonly record struct DispatcherSeriesPoint(
    TimeSpan Elapsed,
    int InFlight,
    long DispatchPending,
    int ActualPending,
    int QueueDepth,
    long QueueBytes);

internal sealed class ConnectionSeriesAggregate
{
    private readonly int _slot;
    private double _pendingSum;
    private int _samples;
    private int _pendingMin = int.MaxValue;
    private int _pendingMax;
    private int _maxInFlight;
    private long _failures;
    private long _reconnects;
    private double _submitRateSum;
    private double _completeRateSum;
    private double _responseRateSum;

    internal ConnectionSeriesAggregate(int slot)
    {
        _slot = slot;
    }

    internal void Observe(TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot, double submitRate, double completeRate, double responseRate, long reconnects)
    {
        _samples++;
        _pendingSum += snapshot.CurrentConcurrentSubmissions;
        _pendingMin = Math.Min(_pendingMin, snapshot.CurrentConcurrentSubmissions);
        _pendingMax = Math.Max(_pendingMax, snapshot.CurrentConcurrentSubmissions);
        _maxInFlight = Math.Max(_maxInFlight, snapshot.MaxConcurrentSubmissions);
        _submitRateSum += submitRate;
        _completeRateSum += completeRate;
        _responseRateSum += responseRate;
        _failures = snapshot.SubmissionsFailed + snapshot.SubmissionsUnavailable;
        _reconnects = reconnects;
    }

    internal string FormatLine()
    {
        double avgPending = _samples == 0 ? 0 : _pendingSum / _samples;
        double avgSubmitRate = _samples == 0 ? 0 : _submitRateSum / _samples;
        double avgCompleteRate = _samples == 0 ? 0 : _completeRateSum / _samples;
        double avgResponseRate = _samples == 0 ? 0 : _responseRateSum / _samples;
        int pendingMin = _pendingMin == int.MaxValue ? 0 : _pendingMin;
        return $"slot={_slot}, pending min/avg/max={pendingMin}/{avgPending:F2}/{_pendingMax}, maxInFlight={_maxInFlight}, submitRate={avgSubmitRate:F2}/s, completionRate={avgCompleteRate:F2}/s, responseRate={avgResponseRate:F2}/s, failures={_failures}, reconnects={_reconnects}";
    }
}

internal static class LatencyAggregators
{
    internal static string BuildConnectionSeriesSummary(Dictionary<int, ConnectionSeriesAggregate> series)
    {
        if (series.Count == 0)
        {
            return "(no connection time-series samples)";
        }

        IEnumerable<string> lines = series
            .OrderBy(static x => x.Key)
            .Select(static kv => kv.Value.FormatLine());

        return string.Join(Environment.NewLine, lines);
    }

    internal static string BuildDispatcherSeriesSummary(List<DispatcherSeriesPoint> series)
    {
        if (series.Count == 0)
        {
            return "(no dispatcher time-series samples)";
        }

        double avgInFlight = series.Average(static x => x.InFlight);
        double avgDispatchPending = series.Average(static x => x.DispatchPending);
        double avgActualPending = series.Average(static x => x.ActualPending);
        int maxInFlight = series.Max(static x => x.InFlight);
        long maxDispatchPending = series.Max(static x => x.DispatchPending);
        int maxActualPending = series.Max(static x => x.ActualPending);

        return $"samples={series.Count}, inFlight avg/max={avgInFlight:F2}/{maxInFlight}, dispatchPending avg/max={avgDispatchPending:F2}/{maxDispatchPending}, actualPending avg/max={avgActualPending:F2}/{maxActualPending}";
    }

    internal static void UpdatePeak(ref long location, long candidate)
    {
        while (true)
        {
            long current = Interlocked.Read(ref location);
            if (candidate <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref location, candidate, current) == current)
            {
                return;
            }
        }
    }

    internal static void UpdateMin(ref long location, long candidate)
    {
        while (true)
        {
            long current = Interlocked.Read(ref location);
            if (candidate >= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref location, candidate, current) == current)
            {
                return;
            }
        }
    }
}
