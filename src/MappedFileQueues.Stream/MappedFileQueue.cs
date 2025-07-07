namespace MappedFileQueues.Stream;

public class MappedFileQueue : IDisposable
{
    private readonly MappedFileQueueOptions _options;

    private MappedFileProducer? _producer;
    private MappedFileConsumer? _consumer;

    public MappedFileQueue(MappedFileQueueOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.StorePath, nameof(options.StorePath));

        if (File.Exists(options.StorePath))
        {
            throw new ArgumentException($"The path '{options.StorePath}' is a file, not a directory.",
                nameof(options.StorePath));
        }

        if (!Directory.Exists(options.StorePath))
        {
            Directory.CreateDirectory(options.StorePath);
        }

        if (options.SegmentSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SegmentSize),
                "SegmentSize must be greater than zero.");
        }

        _options = options;
    }

    public IMappedFileProducer Producer => _producer ??= new MappedFileProducer(_options);

    public IMappedFileConsumer Consumer => _consumer ??= new MappedFileConsumer(_options);


    public void Dispose()
    {
        _producer?.Dispose();
        _consumer?.Dispose();
    }

    public static MappedFileQueue Create(MappedFileQueueOptions options) => new(options);
}
