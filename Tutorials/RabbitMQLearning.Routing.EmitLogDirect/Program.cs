using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
var properties = channel.CreateBasicProperties();
properties.Persistent = true;
channel.ExchangeDeclare("direct_logs", ExchangeType.Direct);

string? message;
do
{
    Console.Write("Input your message (or leave empty to exit): ");
    message = Console.ReadLine();
    if (string.IsNullOrEmpty(message)) break;
    Console.Write("Input color to direct: ");
    var color = Console.ReadLine();

    var body = Encoding.UTF8.GetBytes(message);
    channel.BasicPublish(exchange: "direct_logs",
                         routingKey: color,
                         basicProperties: null,
                         body: body);
} while (!string.IsNullOrEmpty(message));