namespace MappedFileQueues.Stream;

internal class MappedFileProducer : IMappedFileProducer, IDisposable
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the producer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly string _segmentDirectory;

    private MappedFileSegment? _segment;

    private bool _disposed;

    public MappedFileProducer(MappedFileQueueOptions options)
    {
        _options = options;

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ProducerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);
    }

    public void Produce(ReadOnlySpan<byte> message)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (message.Length > Constants.MaxMessageBodySize)
        {
            throw new ArgumentOutOfRangeException(nameof(message),
                $"Message size exceeds the maximum allowed size of {Constants.MaxMessageBodySize} bytes.");
        }

        var bytesToWrite = Constants.MessageHeaderSize + message.Length + Constants.EndMarkerSize;

        EnsureSegmentAvailable(bytesToWrite);

        Span<byte> buffer = stackalloc byte[bytesToWrite];

        // Write the message length at the beginning as the header
        BitConverter.TryWriteBytes(buffer, message.Length);
        // Copy the message into the buffer after the header
        message.CopyTo(buffer[Constants.MessageHeaderSize..]);
        // Write the end marker at the end
        buffer[^Constants.EndMarkerSize..].Fill(Constants.EndMarker);

        _segment.Write(buffer);

        Commit(bytesToWrite);
    }

    public void Produce<T>(T message, IMessageSerializer<T> serializer) => Produce(serializer.Serialize(message));

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

    private void Commit(int bytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_segment == null)
        {
            throw new InvalidOperationException("Segment is not initialized.");
        }

        _offsetFile.Advance(bytes);

        // Check if the segment has reached its limit
        if (!_segment.HasEnoughSpace(Constants.MinMessageSize))
        {
            // Dispose the current segment and will create a new one on the next Produce call
            _segment.Dispose();
            _segment = null;
        }
    }

    private void EnsureSegmentAvailable(int bytesToWrite)
    {
        // If a current segment exists and has enough space, do nothing
        if (_segment != null && _segment.HasEnoughSpace(bytesToWrite))
        {
            return;
        }

        var previousSegmentExistsButEnded = _segment != null;
        // Write end marker if there is space, then dispose the old segment
        if (_segment != null)
        {
            if (_segment.HasEnoughSpace(Constants.MinMessageSize))
            {
                _segment.Write(Constants.FileEndMarker);
            }

            _segment.Dispose();
            _segment = null;
        }

        if (previousSegmentExistsButEnded)
        {
            _segment = MappedFileSegment.Create(
                _segmentDirectory,
                _options.SegmentSize,
                _offsetFile.Offset);
            return;
        }

        // Try to find an existing segment or create a new one
        if (MappedFileSegment.TryFind(_segmentDirectory, _options.SegmentSize, _offsetFile.Offset,
                out var foundSegment))
        {
            if (foundSegment.HasEnoughSpace(bytesToWrite))
            {
                _segment = foundSegment;
                return;
            }

            if (foundSegment.HasEnoughSpace(Constants.MinMessageSize))
            {
                // Write end marker if there is space, then dispose the old segment
                foundSegment.Write(Constants.FileEndMarker);
            }

            foundSegment.Dispose();
        }

        // If the segment was not found or does not have enough space, create a new segment
        _segment = MappedFileSegment.Create(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset);
    }
}
