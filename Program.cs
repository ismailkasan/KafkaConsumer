using System;
using Confluent.Kafka;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
           //var consumergroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP");
            var topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
            var brokerList = Environment.GetEnvironmentVariable("KAFKA_URL");

            var config = new ConsumerConfig {GroupId = "KafkaConsumer", BootstrapServers = "localhost:9092"};

            using (var consumer = new ConsumerBuilder<string, string>(config)
                .SetRebalanceHandler( (obj, e) => {
                    if (e.IsAssignment) Console.WriteLine($"Assigned partitions: [{string.Join(", ", e.Partitions)}]");
                    else Console.WriteLine($"Revoked partitions: [{string.Join(", ", e.Partitions)}]");
                }).Build())
            {
                consumer.Subscribe("ToDoList");
                while (true)
                {
                    ConsumeResult<string, string> consumeResult = consumer.Consume();
                    Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");
                    consumer.Commit();
                }
            }
        }
    }
}
