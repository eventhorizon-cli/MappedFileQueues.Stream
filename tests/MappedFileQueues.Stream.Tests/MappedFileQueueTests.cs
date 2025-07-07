using System.Diagnostics;
using ConsoleApp;
using MappedFileQueues.Tests;

namespace MappedFileQueues.Stream.Tests;

public class MappedFileQueueTests
{
    [Fact]
    public void Produce_Then_Consume()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path, SegmentSize = 33
        };

        using var queue = MappedFileQueue.Create(options);

        var producer = queue.Producer;
        var consumer = queue.Consumer;

        var serializer = new TestMessageSerializer();
        var deserializer = new TestMessageDeserializer();

        for (var i = 0; i < 10; i++)
        {
            var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };

            producer.Produce(testItem, serializer);
        }

        for (var i = 0; i < 10; i++)
        {
            var testItem = consumer.Consume(deserializer);
            consumer.Commit();

            Assert.NotNull(testItem);

            Assert.Equal(i, testItem.A);
            Assert.Equal(i + 1, testItem.B);
            Assert.Equal(i + 2, testItem.C);
            Assert.Equal(i + 3, testItem.D);
        }
    }

    [Fact]
    public async Task Consume_Then_Produce()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 1024,
            ConsumerRetryInterval = TimeSpan.FromMilliseconds(100),
            ConsumerSpinWaitDuration = TimeSpan.FromMilliseconds(50)
        };

        using var queue = MappedFileQueue.Create(options);

        var consumer = queue.Consumer;
        var producer = queue.Producer;

        var serializer = new TestMessageSerializer();
        var deserializer = new TestMessageDeserializer();

        var consumeTask = Task.Run(() =>
        {
            for (var i = 0; i < 10; i++)
            {
                var testItem = consumer.Consume(deserializer);
                consumer.Commit();

                Assert.NotNull(testItem);
                Assert.Equal(i, testItem.A);
                Assert.Equal(i + 1, testItem.B);
                Assert.Equal(i + 2, testItem.C);
                Assert.Equal(i + 3, testItem.D);
            }
        });


        for (var i = 0; i < 10; i++)
        {
            await Task.Delay(200); // for testing the spin wait in the consumer
            var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };

            producer.Produce(testItem, serializer);
        }

        await consumeTask;
    }

    [Fact]
    public void Consume_From_Last_Offset()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        var serializer = new TestMessageSerializer();
        var deserializer = new TestMessageDeserializer();

        using (var queue1 = MappedFileQueue.Create(options))
        {
            var producer1 = queue1.Producer;

            for (var i = 0; i < 10; i++)
            {
                var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer1.Produce(testItem, serializer);
            }

            var consumer1 = queue1.Consumer;

            for (var i = 0; i < 3; i++)
            {
                var testItem = consumer1.Consume(deserializer);
                consumer1.Commit();

                Assert.NotNull(testItem);
                Assert.Equal(i, testItem.A);
                Assert.Equal(i + 1, testItem.B);
                Assert.Equal(i + 2, testItem.C);
                Assert.Equal(i + 3, testItem.D);
            }
        }

        using (var queue2 = MappedFileQueue.Create(options))
        {
            var consumer2 = queue2.Consumer;

            // Consume from the last offset
            for (var i = 3; i < 10; i++)
            {
                var testItem = consumer2.Consume(deserializer);
                consumer2.Commit();

                Assert.NotNull(testItem);
                Assert.Equal(i, testItem.A);
                Assert.Equal(i + 1, testItem.B);
                Assert.Equal(i + 2, testItem.C);
                Assert.Equal(i + 3, testItem.D);
            }
        }
    }

    [Fact]
    public void Produce_From_Last_Offset()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions { StorePath = tempStorePath.Path, SegmentSize = 32 };

        var serializer = new TestMessageSerializer();
        var deserializer = new TestMessageDeserializer();

        using (var queue1 = MappedFileQueue.Create(options))
        {
            var producer1 = queue1.Producer;

            for (var i = 0; i < 10; i++)
            {
                var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer1.Produce(testItem, serializer);
            }
        }

        using (var queue2 = MappedFileQueue.Create(options))
        {
            var producer2 = queue2.Producer;

            for (var i = 10; i < 15; i++)
            {
                var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };
                producer2.Produce(testItem, serializer);
            }

            var consumer2 = queue2.Consumer;

            for (var i = 0; i < 15; i++)
            {
                var testItem = consumer2.Consume(deserializer);
                consumer2.Commit();

                Assert.NotNull(testItem);
                Assert.Equal(i, testItem.A);
                Assert.Equal(i + 1, testItem.B);
                Assert.Equal(i + 2, testItem.C);
                Assert.Equal(i + 3, testItem.D);
            }
        }
    }

    [Fact]
    public void Consumer_Can_Retry_If_Offset_Not_Commited()
    {
        using var tempStorePath = TempStorePath.Create();

        var options = new MappedFileQueueOptions
        {
            StorePath = tempStorePath.Path,
            SegmentSize = 32,
            ConsumerRetryInterval = TimeSpan.FromMilliseconds(100),
            ConsumerSpinWaitDuration = TimeSpan.FromMilliseconds(50)
        };

        using var queue = MappedFileQueue.Create(options);

        var serializer = new TestMessageSerializer();
        var deserializer = new TestMessageDeserializer();

        var producer = queue.Producer;

        for (var i = 0; i < 5; i++)
        {
            var testItem = new TestClass { A = i, B = i + 1, C = i + 2, D = i + 3 };
            producer.Produce(testItem, serializer);
        }

        var consumer = queue.Consumer;

        // Consume without committing
        for (var i = 0; i < 5; i++)
        {
            var testItem = consumer.Consume(deserializer);

            Assert.NotNull(testItem);
            Assert.Equal(0, testItem.A);
            Assert.Equal(1, testItem.B);
            Assert.Equal(2, testItem.C);
            Assert.Equal(3, testItem.D);
        }

        // Consume again, this time committing the offset
        for (var i = 0; i < 5; i++)
        {
            var testItem = consumer.Consume(deserializer);
            consumer.Commit();

            Assert.NotNull(testItem);
            Assert.Equal(i, testItem.A);
            Assert.Equal(i + 1, testItem.B);
            Assert.Equal(i + 2, testItem.C);
            Assert.Equal(i + 3, testItem.D);
        }
    }
}