using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace MappedFileQueues.Stream;

internal class MappedFileConsumer : IMappedFileConsumer, IDisposable
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the consumer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly string _segmentDirectory;
    private MappedFileSegment? _segment;

    private long? _offsetToCommit;
    private byte[]? _pooledBuffer;
    private Memory<byte>? _cachedMessageForRetry;

    private bool _disposed;

    public MappedFileConsumer(MappedFileQueueOptions options)
    {
        _options = options;

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ConsumerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public long Offset => _offsetFile.Offset;

    public void AdjustOffset(long offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater than or equal to zero.");
        }

        if (_segment != null)
        {
            throw new InvalidOperationException(
                "Cannot adjust offset while there is an active segment. Please adjust the offset before consuming any messages.");
        }

        _offsetFile.MoveTo(offset);
    }

    public ReadOnlySpan<byte> Consume()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_offsetToCommit.HasValue)
        {
            // If there is an uncommitted offset, return the cached message
            Debug.Assert(_cachedMessageForRetry.HasValue,
                "Cached message should be available when there is an uncommitted offset.");
            return _cachedMessageForRetry.Value.Span;
        }

        var retryIntervalMs = (int)_options.ConsumerRetryInterval.TotalMilliseconds;
        var spinWaitDurationMs = (int)_options.ConsumerSpinWaitDuration.TotalMilliseconds;

        EnsureSegmentAvailable();

        var spinWait = new SpinWait();
        var startTicks = DateTime.UtcNow.Ticks;

        ReadOnlySpan<byte> payloadBuffer;
        while (!TryRead(out payloadBuffer))
        {
            // Spin wait until the item is available or timeout
            if ((DateTime.UtcNow.Ticks - startTicks) / TimeSpan.TicksPerMillisecond > spinWaitDurationMs)
            {
                // Sleep for a short interval before retrying if spin wait times out
                Thread.Sleep(retryIntervalMs);
            }

            // Use SpinWait to avoid busy waiting
            spinWait.SpinOnce();
        }

        return payloadBuffer;
    }

    public T Consume<T>(IMessageDeserializer<T> deserializer) => deserializer.Deserialize(Consume());

    public void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_offsetToCommit.HasValue)
        {
            throw new InvalidOperationException("Cannot commit without a pending offset to commit.");
        }

        if (_segment == null)
        {
            throw new InvalidOperationException(
                $"No matched segment found. Ensure {nameof(Consume)} is called before {nameof(Commit)}.");
        }

        _offsetFile.MoveTo(_offsetToCommit.Value);
        _offsetToCommit = null;
        ArrayPool<byte>.Shared.Return(_pooledBuffer!);
        _cachedMessageForRetry = null;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _offsetFile.Dispose();
        _segment?.Dispose();
    }


    private void EnsureSegmentAvailable()
    {
        var retryIntervalMs = (int)_options.ConsumerRetryInterval.TotalMilliseconds;
        while (_segment == null)
        {
            if (!TryFindSegmentByOffset(out _segment))
            {
                Thread.Sleep(retryIntervalMs);
            }
            else
            {
                if (!_segment.HasEnoughSpace(Constants.MinMessageSize))
                {
                    // No enough space in the current segment, dispose it and try to find a new one
                    _segment.Dispose();
                    _segment = null;
                    Thread.Sleep(retryIntervalMs);
                }
            }
        }
    }

    private bool TryFindSegmentByOffset([MaybeNullWhen(false)] out MappedFileSegment segment) =>
        MappedFileSegment.TryFind(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset,
            out segment);

    private bool TryRead(out ReadOnlySpan<byte> payloadBuffer)
    {
        var headerSize = Constants.MessageHeaderSize;
        Span<byte> headerBuffer = stackalloc byte[headerSize];

        while (true)
        {
            if (_offsetToCommit.HasValue)
            {
                throw new InvalidOperationException("Cannot read while there is an uncommitted offset.");
            }

            if (_segment == null)
            {
                throw new InvalidOperationException("No segment available to read from.");
            }

            if (!_segment.HasEnoughSpace(headerSize))
            {
                // No enough space in the current segment, dispose it and try to find a new one
                _segment.Dispose();
                _segment = null;
                EnsureSegmentAvailable();
                continue;
            }

            _segment.Read(headerBuffer);

            var endOfSegment = headerBuffer[0] == Constants.SegmentEndMarker;
            if (endOfSegment)
            {
                _segment.Dispose();
                _segment = null;
                EnsureSegmentAvailable();
                continue;
            }

            break;
        }

        var messageLength = BitConverter.ToInt32(headerBuffer);

        if (messageLength <= 0)
        {
            // The next message is not available yet
            payloadBuffer = ReadOnlySpan<byte>.Empty;
            _segment.Rewind(headerSize);

            return false;
        }

        Span<byte> payloadBufferWithEndMarker = stackalloc byte[messageLength + Constants.EndMarkerSize];

        _segment.Read(payloadBufferWithEndMarker);

        var endMarker = payloadBufferWithEndMarker[^1];

        if (endMarker != Constants.EndMarker)
        {
            // The next message is not available yet
            payloadBuffer = ReadOnlySpan<byte>.Empty;
            _segment.Rewind(headerSize + messageLength + Constants.EndMarkerSize);
            return false;
        }

        _pooledBuffer = ArrayPool<byte>.Shared.Rent(messageLength);

        payloadBufferWithEndMarker[..messageLength].CopyTo(_pooledBuffer);

        _offsetToCommit = _offsetFile.Offset + headerSize + messageLength + Constants.EndMarkerSize;
        payloadBuffer = new ReadOnlySpan<byte>(_pooledBuffer, 0, messageLength);
        _cachedMessageForRetry = new Memory<byte>(_pooledBuffer, 0, messageLength);

        return true;
    }
}
