﻿using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare(queue: "hello",
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
string? message;
do
{
    Console.Write("Input your message (or leave empty to exit): ");
    message = Console.ReadLine();
    if (string.IsNullOrEmpty(message)) break;

    var body = Encoding.UTF8.GetBytes(message);
    channel.BasicPublish(exchange: string.Empty,
                         routingKey: "hello",
                         basicProperties: null,
                         body: body);
    Console.WriteLine($" [x] Sent {message}");

    Console.WriteLine(" Press [enter] to continue.");
    Console.ReadLine();
} while (!string.IsNullOrEmpty(message));