namespace MappedFileQueues.Stream;

public interface IMappedFileProducer
{
    /// <summary>
    /// The next offset where message will be written in the mapped file queue.
    /// </summary>
    public long NextOffset { get; }

    /// <summary>
    /// Produces a message to the mapped file queue.
    /// </summary>
    /// <param name="buffer">The byte buffer containing the message to produce.</param>
    public void Produce(ReadOnlySpan<byte> buffer);

    /// <summary>
    /// Produces a message to the mapped file queue using the specified serializer.
    /// </summary>
    /// <param name="message">The message to produce.</param>
    /// <param name="serializer">The serializer to use for the message.</param>
    /// <typeparam name="T">The type of the message to produce.</typeparam>
    public void Produce<T>(T message, IMessageSerializer<T> serializer);
}
