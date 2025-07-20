using ConsoleApp;
using MappedFileQueues.Stream;

var storePath = "test";

// If you have run the test before, delete the previous data first
if (Directory.Exists(storePath))
{
    Directory.Delete(storePath, true);
}

var serializer = new TestMessageSerializer();
var deserializer = new TestMessageDeserializer();

var queue = MappedFileQueue.Create(new MappedFileQueueOptions
{
    StorePath = storePath,
    SegmentSize = 512 * 1024 * 1024 // 512 MB
});

var producer = queue.Producer;

var consumer = queue.Consumer;

var produceTask = Task.Run(() =>
{
    for (var i = 1; i <= 100; i++)
    {
        var testData = new TestClass
        {
            IntValue = i,
            LongValue = i * 10,
            DoubleValue = i / 2.0,
            StringValue = "TestString_" + i
        };
        producer.Produce(testData, serializer);
    }

    Console.WriteLine("Produced 100 items.");
});

var consumeTask = Task.Run(() =>
{
    for (var i = 1; i <= 100; i++)
    {
        var testData = consumer.Consume<TestClass>(deserializer);
        Console.WriteLine(
            $"Consumed: IntValue={testData.IntValue}, LongValue={testData.LongValue}, DoubleValue={testData.DoubleValue}, StringValue={testData.StringValue}");
        consumer.Commit();
    }

    Console.WriteLine("Consumed 100 items.");
});

await Task.WhenAll(produceTask, consumeTask);
