// <copyright file="TransitPublisherBenchmarkLogger.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Diagnostics/TransitPublisherBenchmarkLogger: provides focused diagnostic execution and logging for transit benchmarks.

using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the transit PublisherBenchmarkLogger class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class TransitPublisherBenchmarkLogger : ILogger<TransitPublisher>
{
    /// <summary>
    /// Gets or sets the _inner value.
    /// </summary>
    private readonly ILogger _inner;

    /// <summary>
    /// Performs the transit PublisherBenchmarkLogger operation.
    /// </summary>
    internal TransitPublisherBenchmarkLogger(ILogger inner)
    {
        _inner = inner;
    }

    /// <summary>
    /// Creates a logging scope through the wrapped logger so benchmark code preserves the
    /// ambient logging context supplied by the production publisher.
    /// </summary>
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return _inner.BeginScope(state);
    }

    /// <summary>
    /// Performs the is Enabled operation.
    /// </summary>
    public bool IsEnabled(LogLevel logLevel)
    {
        return _inner.IsEnabled(logLevel);
    }

    /// <summary>
    /// Forwards an enabled log event after applying the benchmark-specific suppression rule for
    /// high-volume accepted-article noise.
    /// </summary>
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!_inner.IsEnabled(logLevel))
        {
            return;
        }

        string message = formatter(state, exception);
        if (ShouldSuppressAccepted239Spam(eventId, logLevel, message))
        {
            return;
        }

        _inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>
    /// Performs the should SuppressAccepted239 Spam operation.
    /// </summary>
    private static bool ShouldSuppressAccepted239Spam(EventId eventId, LogLevel level, string message)
    {
        if (message.Contains("[INIT-TRACE]", StringComparison.Ordinal))
        {
            return true;
        }

        if (level != LogLevel.Information)
        {
            return false;
        }

        if (eventId.Id == 2203)
        {
            return true;
        }

        if (eventId.Id != 2204)
        {
            return false;
        }

        if (!message.Contains("Status=Accepted", StringComparison.Ordinal))
        {
            return false;
        }

        return message.Contains("ResponseCode=239", StringComparison.Ordinal);
    }
}
