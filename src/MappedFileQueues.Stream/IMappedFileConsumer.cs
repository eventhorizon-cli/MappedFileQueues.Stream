namespace MappedFileQueues.Stream;

public interface IMappedFileConsumer
{
    /// <summary>
    /// Consumes a message from the mapped file queue.
    /// </summary>
    /// <remarks>Do not use the returned span after calling Commit.</remarks>
    /// <returns>A span containing the consumed message.</returns>
    public ReadOnlySpan<byte> Consume();

    /// <summary>
    /// Consumes a message from the mapped file queue and deserializes it using the provided deserializer.
    /// </summary>
    /// <param name="deserializer">The deserializer to use for the message.</param>
    /// <typeparam name="T">The type of the message to deserialize.</typeparam>
    /// <returns>The deserialized message of type T.</returns>
    public T Consume<T>(IMessageDeserializer<T> deserializer);

    /// <summary>
    /// Commits the offset of the last consumed message.
    /// </summary>
    void Commit();
}
