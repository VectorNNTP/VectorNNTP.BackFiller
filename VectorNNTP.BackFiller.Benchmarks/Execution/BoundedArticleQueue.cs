using System.Diagnostics;
using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

internal sealed class BoundedArticleQueue : IDisposable
{
    private readonly Channel<QueuedArticle> _channel;
    private readonly ByteBudget _byteBudget;
    private readonly QueueConsumerForensics? _forensics;
    private long _queuedBytes;
    private int _queuedCount;
    private volatile bool _admissionStopped;

    internal BoundedArticleQueue(int maxArticles, long maxResidentBytes, QueueConsumerForensics? forensics = null)
    {
        _forensics = forensics;
        _channel = Channel.CreateBounded<QueuedArticle>(new BoundedChannelOptions(maxArticles)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        _byteBudget = new ByteBudget(maxResidentBytes);
    }

    internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
    internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

    internal async ValueTask<bool> TryWriteAsync(QueuedArticle article, CancellationToken cancellationToken)
    {
        if (_admissionStopped)
        {
            return false;
        }

        await _byteBudget.AcquireAsync(article.PayloadLength, cancellationToken).ConfigureAwait(false);

        try
        {
            long channelWriteStartTicks = _forensics is null ? 0 : Stopwatch.GetTimestamp();
            await _channel.Writer.WriteAsync(article, cancellationToken).ConfigureAwait(false);
            long channelWriteCompletedTicks = _forensics is null ? 0 : Stopwatch.GetTimestamp();
            long threadPoolPendingAtWrite = _forensics is null ? 0 : ThreadPool.PendingWorkItemCount;
            Interlocked.Increment(ref _queuedCount);
            Interlocked.Add(ref _queuedBytes, article.PayloadLength);
            _forensics?.RecordEnqueue(channelWriteStartTicks, channelWriteCompletedTicks, threadPoolPendingAtWrite, Stopwatch.GetTimestamp());
            return true;
        }
        catch
        {
            _byteBudget.Release(article.PayloadLength);
            throw;
        }
    }

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

    internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }

    internal void ReleaseReservation(int bytes)
    {
        _byteBudget.Release(bytes);
    }

    internal void StopAdmission()
    {
        _admissionStopped = true;
        _channel.Writer.TryComplete();
    }

    public void Dispose()
    {
        StopAdmission();
        _byteBudget.Dispose();
    }
}
