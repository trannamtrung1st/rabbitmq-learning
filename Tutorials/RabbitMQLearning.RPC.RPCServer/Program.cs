
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQLearning.RPC.Shared;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

// [NOTE] we can use default exchange instead
string exchangeName = "rpc_exchange";
channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);

string queueName = channel.QueueDeclare(queue: "rpc_random",
                                        durable: false,
                                        exclusive: false,
                                        autoDelete: false).QueueName;
channel.QueueBind(queue: queueName,
                  exchange: exchangeName,
                  routingKey: queueName);

channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    RpcResponse<int> response = new();
    var props = ea.BasicProperties;
    var replyProps = channel.CreateBasicProperties();
    replyProps.CorrelationId = props.CorrelationId;

    try
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine($" [x] Received {message}");
        if (int.TryParse(message, out int max))
        {
            int random = Random.Shared.Next(0, max);
            Thread.Sleep(random * 1000);
            response.Successful = true;
            response.Data = random;
        }
        else
        {
            response.Successful = false;
            response.Message = "Invalid request";
        }
    }
    catch (Exception e)
    {
        response.Successful = false;
        response.Message = e.Message;
    }
    finally
    {
        var responseStr = JsonSerializer.Serialize(response);
        Console.WriteLine($" [x] Returning to {props.ReplyTo}: {responseStr}");
        var responseBytes = Encoding.UTF8.GetBytes(responseStr);
        channel.BasicPublish(exchange: exchangeName,
                             routingKey: props.ReplyTo,
                             basicProperties: replyProps,
                             body: responseBytes);
        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
    }
};

channel.BasicConsume(queue: queueName,
                     autoAck: false,
                     consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();