namespace MappedFileQueues.Stream;

public interface IMessageDeserializer<out T>
{
    public T Deserialize(ReadOnlySpan<byte> buffer);
}
