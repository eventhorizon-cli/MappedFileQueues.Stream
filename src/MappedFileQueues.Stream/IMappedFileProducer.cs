namespace MappedFileQueues.Stream;

public interface IMappedFileProducer
{
    public void Produce(ReadOnlySpan<byte> buffer);

    public void Produce<T>(T item, IMessageSerializer<T> serializer);
}
