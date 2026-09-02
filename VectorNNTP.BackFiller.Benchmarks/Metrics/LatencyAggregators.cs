// <copyright file="LatencyAggregators.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/LatencyAggregators: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the connection CounterState record struct used by this benchmark or regression-gate component.
/// </summary>
internal readonly record struct ConnectionCounterState(
    string ConnectionId,
    TimeSpan Elapsed,
    long SubmissionsStarted,
    long Completed);

/// <summary>
/// Represents the dispatcher SeriesPoint record struct used by this benchmark or regression-gate component.
/// </summary>
internal readonly record struct DispatcherSeriesPoint(
    TimeSpan Elapsed,
    int InFlight,
    long DispatchPending,
    int ActualPending,
    int QueueDepth,
    long QueueBytes);

/// <summary>
/// Represents the connection SeriesAggregate class used by this benchmark or regression-gate component.
/// </summary>
internal sealed class ConnectionSeriesAggregate
{
    /// <summary>
    /// Gets or sets the _slot value used by this component.
    /// </summary>
    private readonly int _slot;
    /// <summary>
    /// Gets or sets the _pendingSum value used by this component.
    /// </summary>
    private double _pendingSum;
    /// <summary>
    /// Gets or sets the _samples value used by this component.
    /// </summary>
    private int _samples;
    /// <summary>
    /// Gets or sets the _pendingMin value used by this component.
    /// </summary>
    private int _pendingMin = int.MaxValue;
    /// <summary>
    /// Gets or sets the _pendingMax value used by this component.
    /// </summary>
    private int _pendingMax;
    /// <summary>
    /// Gets or sets the _maxInFlight value used by this component.
    /// </summary>
    private int _maxInFlight;
    /// <summary>
    /// Gets or sets the _failures value used by this component.
    /// </summary>
    private long _failures;
    /// <summary>
    /// Gets or sets the _reconnects value used by this component.
    /// </summary>
    private long _reconnects;
    /// <summary>
    /// Gets or sets the _submitRateSum value used by this component.
    /// </summary>
    private double _submitRateSum;
    /// <summary>
    /// Gets or sets the _completeRateSum value used by this component.
    /// </summary>
    private double _completeRateSum;
    /// <summary>
    /// Gets or sets the _responseRateSum value used by this component.
    /// </summary>
    private double _responseRateSum;

    /// <summary>
    /// Executes the connection SeriesAggregate operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal ConnectionSeriesAggregate(int slot)
    {
        _slot = slot;
    }

    /// <summary>
    /// Executes the observe operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    /// <summary>
    /// Executes the format Line operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

/// <summary>
/// Represents the latency Aggregators class used by this benchmark or regression-gate component.
/// </summary>
internal static class LatencyAggregators
{
    /// <summary>
    /// Executes the build ConnectionSeriesSummary operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    /// <summary>
    /// Executes the build DispatcherSeriesSummary operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    /// <summary>
    /// Executes the update Peak operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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

    /// <summary>
    /// Executes the update Min operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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
