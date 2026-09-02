// <copyright file="BoundedArticleQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/BoundedArticleQueue: bounds queued articles by both item count and payload bytes.

using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the bounded ArticleQueue class used by the benchmark or regression gate.
/// </summary>
internal sealed class BoundedArticleQueue : IDisposable
{
    /// <summary>
    /// Holds the bounded channel that enforces queued-article capacity.
    /// </summary>
    private readonly Channel<QueuedArticle> _channel;
    /// <summary>
    /// Gets or sets the _byteBudget.
    /// </summary>
    private readonly ByteBudget _byteBudget;
    /// <summary>
    /// Gets or sets the _queuedBytes.
    /// </summary>
    private long _queuedBytes;
    /// <summary>
    /// Gets or sets the _queuedCount.
    /// </summary>
    private int _queuedCount;
    /// <summary>
    /// Gets or sets the _admissionStopped.
    /// </summary>
    private volatile bool _admissionStopped;

    /// <summary>
    /// Implements the bounded ArticleQueue contract.
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
    /// Implements the current QueuedCount contract.
    /// </summary>
    internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
    /// <summary>
    /// Implements the current QueuedBytes contract.
    /// </summary>
    internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

    /// <summary>
    /// Implements the try WriteAsync contract.
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
    /// Implements the try Read contract.
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
    /// Implements the wait ToReadAsync contract.
    /// </summary>
    internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }

    /// <summary>
    /// Implements the release Reservation contract.
    /// </summary>
    internal void ReleaseReservation(int bytes)
    {
        _byteBudget.Release(bytes);
    }

    /// <summary>
    /// Stops Admission.

    /// </summary>
    internal void StopAdmission()
    {
        _admissionStopped = true;
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Releases resources held by this instance.
    /// </summary>
    public void Dispose()
    {
        StopAdmission();
        _byteBudget.Dispose();
    }
}
