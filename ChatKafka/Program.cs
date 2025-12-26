using System;
using Confluent.Kafka;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

record ChatMessage(string User, string Text, DateTime Time);

class Program
{
    static async Task Main()
    {
        Console.Write("이름: ");
        var myName = Console.ReadLine();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "211.188.52.188:9092",
            GroupId = $"chat-consumer-{myName}",
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "211.188.52.188:9092",
            Acks = Acks.All
        };

        using var consumer =
            new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

        using var producer =
            new ProducerBuilder<Null, string>(producerConfig).Build();

        consumer.Subscribe("chat.cmd.v1");

        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
            consumer.Close();
        };

        // Consumer
        Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var cr = consumer.Consume(cts.Token);
                    var msg = JsonSerializer.Deserialize<ChatMessage>(cr.Message.Value);
                    if (msg == null || msg.User == myName) continue;

                    Console.WriteLine($"{msg.User}: {msg.Text}");
                }
            }
            catch (OperationCanceledException) { }
        });

        // Producer
        while (!cts.IsCancellationRequested)
        {
            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input)) continue;

            var payload = JsonSerializer.Serialize(
                new ChatMessage(myName, input, DateTime.UtcNow)
            );

            await producer.ProduceAsync(
                "chat.cmd.v1",
                new Message<Null, string> { Value = payload }
            );
        }
    }
}
