using System.Diagnostics;
using ConsoleApp;
using MappedFileQueues.Stream;

// Clean up the test directory
var storePath = "test";
if (Directory.Exists(storePath))
{
    Directory.Delete(storePath, true);
}

var segmentSize = 512 * 1024 * 1024; // 512 MB
var items = 20_000_000;

var serializer = new TestMessageSerializer();
var deserializer = new TestMessageDeserializer();

using var mappedFileQueue = MappedFileQueue.Create(new MappedFileQueueOptions
{
    StorePath = "test", SegmentSize = segmentSize
});

var producer = mappedFileQueue.Producer;
var consumer = mappedFileQueue.Consumer;


var sw = Stopwatch.StartNew();

for (var i = 1; i <= items; i++)
{
    var testItem = new TestClass
    {
        IntValue = i, LongValue = i * 10, DoubleValue = i / 2.0, StringValue = "TestString" + i
    };

    if (i == 1)
    {
        Console.WriteLine($"The first item: {nameof(testItem.IntValue)} = {testItem.IntValue}, " +
                          $"{nameof(testItem.LongValue)} = {testItem.LongValue}, " +
                          $"{nameof(testItem.DoubleValue)} = {testItem.DoubleValue}, " +
                          $"{nameof(testItem.StringValue)} = {testItem.StringValue}");
    }

    if (i == items)
    {
        Console.WriteLine($"The last item: {nameof(testItem.IntValue)} = {testItem.IntValue}, " +
                          $"{nameof(testItem.LongValue)} = {testItem.LongValue}, " +
                          $"{nameof(testItem.DoubleValue)} = {testItem.DoubleValue}, " +
                          $"{nameof(testItem.StringValue)} = {testItem.StringValue}");
    }

    producer.Produce(testItem, serializer);
}

Console.WriteLine($"Completed writing {items} items in {sw.ElapsedMilliseconds} ms");

sw.Restart();
for (var i = 1; i <= items; i++)
{
    var testItem = consumer.Consume<TestClass>(deserializer);
    consumer.Commit();

    if (i == 1)
    {
        Console.WriteLine($"The first item: {nameof(testItem.IntValue)} = {testItem.IntValue}, " +
                          $"{nameof(testItem.LongValue)} = {testItem.LongValue}, " +
                          $"{nameof(testItem.DoubleValue)} = {testItem.DoubleValue}, " +
                          $"{nameof(testItem.StringValue)} = {testItem.StringValue}");
    }

    if (i == items)
    {
        Console.WriteLine($"The last item: {nameof(testItem.IntValue)} = {testItem.IntValue}, " +
                          $"{nameof(testItem.LongValue)} = {testItem.LongValue}, " +
                          $"{nameof(testItem.DoubleValue)} = {testItem.DoubleValue}, " +
                          $"{nameof(testItem.StringValue)} = {testItem.StringValue}");
    }
}

Console.WriteLine($"Completed reading {items} items in {sw.ElapsedMilliseconds} ms");
