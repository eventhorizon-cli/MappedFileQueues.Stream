using System.Text.Json;
using MappedFileQueues.Stream;

namespace ConsoleApp;

public class TestMessageDeserializer : IMessageDeserializer<TestClass?>
{
    public TestClass? Deserialize(ReadOnlySpan<byte> buffer) => JsonSerializer.Deserialize<TestClass>(buffer);
}
