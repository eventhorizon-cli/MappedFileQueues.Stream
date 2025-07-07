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

        int bytesToWrite = Constants.MessageHeaderSize + message.Length + Constants.EndMarkerSize;

        _segment ??= FindOrCreateSegmentByOffset(bytesToWrite);

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

    public void Produce<T>(T message, IMessageSerializer<T> serializer)
    {
        var buffer = serializer.Serialize(message);
        Produce(buffer);
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

    private MappedFileSegment FindOrCreateSegmentByOffset(int bytesToWrite) =>
        MappedFileSegment.FindOrCreate(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset,
            bytesToWrite);
}