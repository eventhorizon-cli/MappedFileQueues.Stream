using System.Buffers;

namespace MappedFileQueues.Stream;

public interface IMessageSerializer<in T>
{
    public ReadOnlySpan<byte> Serialize(T message);
}