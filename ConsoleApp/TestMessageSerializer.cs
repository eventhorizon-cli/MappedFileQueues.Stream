using System.Text.Json;
using MappedFileQueues.Stream;

namespace ConsoleApp;

public class TestMessageSerializer : IMessageSerializer<TestClass>
{
    public ReadOnlySpan<byte> Serialize(TestClass message) => JsonSerializer.SerializeToUtf8Bytes(message);
}