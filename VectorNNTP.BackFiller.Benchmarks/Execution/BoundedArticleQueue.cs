// <copyright file="BoundedArticleQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/BoundedArticleQueue: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the bounded ArticleQueue class used by this benchmark or regression-gate component.
/// </summary>
internal sealed class BoundedArticleQueue : IDisposable
{
    /// <summary>
    /// Gets or sets the _channel value used by this component.
    /// </summary>
    private readonly Channel<QueuedArticle> _channel;
    /// <summary>
    /// Gets or sets the _byteBudget value used by this component.
    /// </summary>
    private readonly ByteBudget _byteBudget;
    /// <summary>
    /// Gets or sets the _queuedBytes value used by this component.
    /// </summary>
    private long _queuedBytes;
    /// <summary>
    /// Gets or sets the _queuedCount value used by this component.
    /// </summary>
    private int _queuedCount;
    /// <summary>
    /// Gets or sets the _admissionStopped value used by this component.
    /// </summary>
    private volatile bool _admissionStopped;

    /// <summary>
    /// Executes the bounded ArticleQueue operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the current QueuedCount operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
    /// <summary>
    /// Executes the current QueuedBytes operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

    /// <summary>
    /// Executes the try WriteAsync operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the try Read operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the wait ToReadAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }

    /// <summary>
    /// Executes the release Reservation operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal void ReleaseReservation(int bytes)
    {
        _byteBudget.Release(bytes);
    }

    /// <summary>
    /// Executes the stop Admission operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal void StopAdmission()
    {
        _admissionStopped = true;
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Executes the dispose operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public void Dispose()
    {
        StopAdmission();
        _byteBudget.Dispose();
    }
}
