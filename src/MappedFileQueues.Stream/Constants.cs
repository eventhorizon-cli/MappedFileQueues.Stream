namespace MappedFileQueues.Stream;

internal class Constants
{
    public const string CommitLogDirectory = "commitlog";

    public const string OffsetDirectory = "offset";

    public const string ProducerOffsetFile = "producer.offset";

    public const string ConsumerOffsetFile = "consumer.offset";

    public const byte MessageHeaderSize = sizeof(int);

    public const byte EndMarker = 0xFF;

    public const byte EndMarkerSize = 1;

    // At least one byte for the message content
    public const byte MinMessageSize = MessageHeaderSize + EndMarkerSize + 1;
}