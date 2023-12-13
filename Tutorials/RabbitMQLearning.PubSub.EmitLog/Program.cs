using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
var properties = channel.CreateBasicProperties();
properties.Persistent = true;
channel.ExchangeDeclare("logs", ExchangeType.Fanout);

string? message;
do
{
    Console.Write("Input your message (or leave empty to exit): ");
    message = Console.ReadLine();
    if (string.IsNullOrEmpty(message)) break;

    var body = Encoding.UTF8.GetBytes(message);
    channel.BasicPublish(exchange: "logs",
                         routingKey: string.Empty,
                         basicProperties: null,
                         body: body);
} while (!string.IsNullOrEmpty(message));