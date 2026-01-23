using System;
using Confluent.Kafka;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

record ChatMessage(string User, string Text, DateTime Time);

class Program
{
    static readonly ConcurrentQueue<string> PrintQueue = new();
    static readonly object ConsoleLock = new();

    static string InputBuffer = "";
    static bool Running = true;

    static async Task Main()
    {
        Console.Write("이름: ");
        var myName = Console.ReadLine() ?? "unknown";

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

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Running = false;
            consumer.Close();
        };

        // =========================
        // Kafka Consumer (수신)
        // =========================
        _ = Task.Run(() =>
        {
            try
            {
                while (Running)
                {
                    var cr = consumer.Consume();
                    var msg = JsonSerializer.Deserialize<ChatMessage>(cr.Message.Value);

                    if (msg == null || msg.User == myName)
                        continue;

                    PrintQueue.Enqueue($"{msg.User}: {msg.Text}");
                }
            }
            catch { }
        });

        // =========================
        // 키 입력 처리
        // =========================
        _ = Task.Run(async () =>
        {
            while (Running)
            {
                var key = Console.ReadKey(true);

                lock (ConsoleLock)
                {
                    if (key.Key == ConsoleKey.Enter)
                    {
                        Console.WriteLine();

                        var text = InputBuffer;
                        InputBuffer = "";

                        if (!string.IsNullOrWhiteSpace(text))
                        {
                            var payload = JsonSerializer.Serialize(
                                new ChatMessage(myName, text, DateTime.UtcNow)
                            );

                            producer.Produce(
                                "chat.cmd.v1",
                                new Message<Null, string> { Value = payload }
                            );
                        }

                        Console.Write("> ");
                    }
                    else if (key.Key == ConsoleKey.Backspace)
                    {
                        if (InputBuffer.Length > 0)
                        {
                            InputBuffer = InputBuffer[..^1];
                            Console.Write("\b \b");
                        }
                    }
                    else
                    {
                        InputBuffer += key.KeyChar;
                        Console.Write(key.KeyChar);
                    }
                }

                await Task.Delay(1);
            }
        });

        // =========================
        // 출력 루프
        // =========================
        Console.WriteLine("(Ctrl+C 종료)");
        Console.Write("> ");

        while (Running)
        {
            while (PrintQueue.TryDequeue(out var msg))
            {
                lock (ConsoleLock)
                {
                    // 현재 입력 줄 제거
                    Console.Write("\r" + new string(' ', Console.WindowWidth) + "\r");

                    // 메시지 출력
                    Console.WriteLine(msg);

                    // 입력 복원
                    Console.Write("> " + InputBuffer);
                }
            }

            Thread.Sleep(50);
        }
    }
}
