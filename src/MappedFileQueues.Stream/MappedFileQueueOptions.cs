namespace MappedFileQueues.Stream;

public class MappedFileQueueOptions
{
    /// <summary>
    /// The path to store the mapped files and other runtime data.
    /// </summary>
    public required string StorePath { get; set; }

    /// <summary>
    /// The size of each mapped file segment in bytes, may be adjusted to fit the data type.
    /// </summary>
    public required long SegmentSize { get; set; }

    /// <summary>
    /// The interval between two spin-wait attempts when consuming items.
    /// </summary>
    public TimeSpan ConsumerRetryInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The maximum duration a consumer will spin-wait each time for an item to become available.
    /// </summary>
    public TimeSpan ConsumerSpinWaitDuration { get; set; } = TimeSpan.FromMilliseconds(100);
}