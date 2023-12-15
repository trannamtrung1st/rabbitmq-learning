using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
channel.ConfirmSelect();

// PublishAndWaitForConfirm(connection, channel);

// PublishBatchAndWaitForConfirm(connection, channel);

ConfirmCallback(connection, channel);

Console.WriteLine("Press enter to exit!");
Console.ReadLine();

static void PublishAndWaitForConfirm(IConnection connection, IModel channel)
{
    var message = PromptMessage();
    var body = Encoding.UTF8.GetBytes(message);
    var properties = channel.CreateBasicProperties();
    channel.BasicPublish(exchange: string.Empty,
                         routingKey: "publisher_confirms",
                         basicProperties: properties,
                         body: body);
    channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
}

static void PublishBatchAndWaitForConfirm(IConnection connection, IModel channel)
{
    var batchSize = 5;
    var outstandingMessageCount = 0;
    string? message;
    do
    {
        message = PromptMessage();
        if (string.IsNullOrEmpty(message)) continue;

        var properties = channel.CreateBasicProperties();
        var body = Encoding.UTF8.GetBytes(message);
        channel.BasicPublish(exchange: string.Empty,
                             routingKey: "publisher_confirms",
                             basicProperties: properties,
                             body: body);
        outstandingMessageCount++;
        if (outstandingMessageCount == batchSize)
        {
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
            outstandingMessageCount = 0;
        }
    } while (!string.IsNullOrEmpty(message));

    if (outstandingMessageCount > 0)
    {
        channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
    }
}

static void ConfirmCallback(IConnection connection, IModel channel)
{
    var outstandingConfirms = new ConcurrentDictionary<ulong, string>();
    string? message;

    void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
    {
        if (outstandingConfirms == null) return;
        if (multiple)
        {
            var confirmed = outstandingConfirms.Where(k => k.Key <= sequenceNumber);
            foreach (var entry in confirmed)
            {
                outstandingConfirms.TryRemove(entry.Key, out _);
            }
        }
        else
        {
            outstandingConfirms.TryRemove(sequenceNumber, out _);
        }
    }

    // [NOTE] callbacks run in different threads handled by client lib
    channel.BasicAcks += (sender, ea) =>
    {
        Console.WriteLine($"\n [*] Acked: {ea.DeliveryTag} - {ea.Multiple}");
        CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
    };
    channel.BasicNacks += (sender, ea) =>
    {
        // [NOTE] shouldn't do republishing here since this is different thread
        if (outstandingConfirms.TryGetValue(ea.DeliveryTag, out string? body))
        {
            Console.WriteLine($"\n [*] Message with body {body} has been nack-ed. Sequence number: {ea.DeliveryTag}, multiple: {ea.Multiple}");
        }
        CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
    };

    do
    {
        message = PromptMessage();
        if (string.IsNullOrEmpty(message)) continue;

        outstandingConfirms.TryAdd(channel.NextPublishSeqNo, message);
        var properties = channel.CreateBasicProperties();
        var body = Encoding.UTF8.GetBytes(message);
        channel.BasicPublish(exchange: string.Empty,
                             routingKey: "publisher_confirms",
                             basicProperties: properties,
                             body: body);
    } while (!string.IsNullOrEmpty(message));
}

static string PromptMessage()
{
    Console.Write("Input your message: ");
    string message = Console.ReadLine() ?? string.Empty;
    return message;
}