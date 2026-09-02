// <copyright file="BoundedArticleQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/BoundedArticleQueue: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the bounded ArticleQueue class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class BoundedArticleQueue : IDisposable
{
    /// <summary>
    /// Gets or sets the _channel value.
    /// </summary>
    private readonly Channel<QueuedArticle> _channel;
    /// <summary>
    /// Gets or sets the _byteBudget value.
    /// </summary>
    private readonly ByteBudget _byteBudget;
    /// <summary>
    /// Gets or sets the _queuedBytes value.
    /// </summary>
    private long _queuedBytes;
    /// <summary>
    /// Gets or sets the _queuedCount value.
    /// </summary>
    private int _queuedCount;
    /// <summary>
    /// Gets or sets the _admissionStopped value.
    /// </summary>
    private volatile bool _admissionStopped;

    /// <summary>
    /// Performs the bounded ArticleQueue operation.
    /// </summary>
    internal BoundedArticleQueue(int maxArticles, long maxResidentBytes)
    {
        _channel = Channel.CreateBounded<QueuedArticle>(new BoundedChannelOptions(maxArticles)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        _byteBudget = new ByteBudget(maxResidentBytes);
    }

    /// <summary>
    /// Performs the current QueuedCount operation.
    /// </summary>
    internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
    /// <summary>
    /// Performs the current QueuedBytes operation.
    /// </summary>
    internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

    /// <summary>
    /// Performs the try WriteAsync operation.
    /// </summary>
    internal async ValueTask<bool> TryWriteAsync(QueuedArticle article, CancellationToken cancellationToken)
    {
        if (_admissionStopped)
        {
            return false;
        }

        await _byteBudget.AcquireAsync(article.PayloadLength, cancellationToken).ConfigureAwait(false);

        try
        {
            await _channel.Writer.WriteAsync(article, cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref _queuedCount);
            Interlocked.Add(ref _queuedBytes, article.PayloadLength);
            return true;
        }
        catch
        {
            _byteBudget.Release(article.PayloadLength);
            throw;
        }
    }

    /// <summary>
    /// Performs the try Read operation.
    /// </summary>
    internal bool TryRead(out QueuedArticle article)
    {
        bool success = _channel.Reader.TryRead(out article);
        if (success)
        {
            Interlocked.Decrement(ref _queuedCount);
            Interlocked.Add(ref _queuedBytes, -article.PayloadLength);
        }

        return success;
    }

    /// <summary>
    /// Performs the wait ToReadAsync operation.
    /// </summary>
    internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }

    /// <summary>
    /// Performs the release Reservation operation.
    /// </summary>
    internal void ReleaseReservation(int bytes)
    {
        _byteBudget.Release(bytes);
    }

    /// <summary>
    /// Performs the stop Admission operation.
    /// </summary>
    internal void StopAdmission()
    {
        _admissionStopped = true;
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Performs the dispose operation.
    /// </summary>
    public void Dispose()
    {
        StopAdmission();
        _byteBudget.Dispose();
    }
}
