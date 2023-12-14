namespace RabbitMQLearning.RPC.Shared;

public class RpcResponse<T>
{
    public bool Successful { get; set; }
    public T? Data { get; set; } = default;
    public string? Message { get; set; }
}