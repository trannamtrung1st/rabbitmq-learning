using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQLearning.RPC.Shared;

var rpcClient = new RpcClient();
string? message;
do
{
    Console.Write("Input max to random (or leave empty to exit): ");
    message = Console.ReadLine();
    if (string.IsNullOrEmpty(message)) break;

    var rpcResponse = await rpcClient.GetRandomAsync(message);
    Console.WriteLine($" [x] Received {JsonSerializer.Serialize(rpcResponse)}");
    if (rpcResponse?.Successful == true)
        Console.WriteLine($" [x] Random number {rpcResponse.Data}");


} while (!string.IsNullOrEmpty(message));

// ===== Class definitions =====

public class RpcClient : IDisposable
{
    private const string RPC_EXCHANGE = "rpc_exchange";
    private const string RPC_RANDOM_QUEUE = "rpc_random";

    private readonly ConcurrentDictionary<string, TaskCompletionSource<RpcResponse<int>?>> _callbackMap;
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _replyQueueName;

    public RpcClient()
    {
        _callbackMap = new();
        var factory = new ConnectionFactory { HostName = "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _replyQueueName = _channel.QueueDeclare().QueueName;

        // [NOTE] we can use default exchange instead without queue bind
        _channel.QueueBind(queue: _replyQueueName,
                           exchange: RPC_EXCHANGE,
                           routingKey: _replyQueueName);

        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (model, ea) =>
        {
            if (!_callbackMap.TryGetValue(ea.BasicProperties.CorrelationId, out var tcs))
                return;

            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var rpcResponse = JsonSerializer.Deserialize<RpcResponse<int>>(message);
                tcs.TrySetResult(rpcResponse);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        };

        _channel.BasicConsume(queue: _replyQueueName,
                              autoAck: true,
                              consumer);
    }

    public Task<RpcResponse<int>?> GetRandomAsync(string requestMessage, CancellationToken cancellationToken = default)
    {
        var properties = _channel.CreateBasicProperties();
        properties.ReplyTo = _replyQueueName;
        string correlationId = Guid.NewGuid().ToString();
        properties.CorrelationId = correlationId;
        var body = Encoding.UTF8.GetBytes(requestMessage);
        var tcs = new TaskCompletionSource<RpcResponse<int>?>();
        _callbackMap[correlationId] = tcs;

        _channel.BasicPublish(exchange: RPC_EXCHANGE,
                              routingKey: RPC_RANDOM_QUEUE,
                              basicProperties: properties,
                              body: body);

        cancellationToken.Register(() => _callbackMap.TryRemove(correlationId, out _));

        return tcs.Task;
    }

    public void Dispose() => _connection?.Dispose();
}