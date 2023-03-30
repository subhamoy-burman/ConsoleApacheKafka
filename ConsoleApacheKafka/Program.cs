using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace ConsoleApacheKafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            
            //create a producer configuration object with the bootstrap servers and schema registry URL.
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            };
            
            //create a schema registry client and a serializer for strings.
            using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            var valueSerializer = new AvroSerializer<string>(schemaRegistry);

            // create a producer builder and a producer object
            var producerBuilder = new ProducerBuilder<Null, string>(producerConfig).SetValueSerializer(valueSerializer);
            using var producer = producerBuilder.Build();

            var logFile = "/Users/burmas3/Codebases/NasaLogs/NASA_access_log_Aug95";//"~/NASA-logs/NASA_access_log_Jul95";
            var topicName = "messages";

            //read the log data from the NASA logs file line by line and produce them to the Kafka topic.
            using (var reader = new StreamReader(logFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var deliveryReport = await producer.ProduceAsync(
                        topicName,
                        new Message<Null, string> { Value = line });
                    Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
                }
            }
        }
    }
}