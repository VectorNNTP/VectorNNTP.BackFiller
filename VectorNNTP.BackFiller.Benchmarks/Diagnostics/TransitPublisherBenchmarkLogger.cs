using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal sealed class TransitPublisherBenchmarkLogger : ILogger<TransitPublisher>
{
    private readonly ILogger _inner;

    internal TransitPublisherBenchmarkLogger(ILogger inner)
    {
        _inner = inner;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return _inner.BeginScope(state);
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return _inner.IsEnabled(logLevel);
    }

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
