using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace FaktoryWorker;

public class FaktoryClient : IAsyncDisposable
{
    private readonly ILogger? _logger;
    private readonly Socket _socket;
    private readonly IPEndPoint _ipEndPoint;
    private readonly string _workerHostName;
    private readonly int _version;
    private readonly string _workerId;

    public FaktoryClient(string host, int port, string workerHostName, int version, string workerId, ILogger? logger = null)
    {
        _workerHostName = workerHostName;
        _version = version;
        _workerId = workerId;
        _logger = logger;
        IPAddress ipAddress;
        try
        {
            var ipHostInfo = Dns.GetHostEntry(host);
            ipAddress = ipHostInfo.AddressList[0];
        }
        catch (Exception)
        {
            ipAddress = IPAddress.Parse(host);
        }

        _ipEndPoint = new IPEndPoint(ipAddress, port);

        _socket = new Socket(
            _ipEndPoint.AddressFamily,
            SocketType.Stream,
            ProtocolType.Tcp);
    }

    public async Task ConnectAsync()
    {
        await _socket.ConnectAsync(_ipEndPoint);

        var response = await ReceiveResponse("+HI");
        response.Switch(
            _ => { },
            _ => throw new Exception("Failed to connect to Faktory"));
    }

    public async Task HeartbeatAsync()
    {
        var beat = new {wid = _workerId};
        var json = JsonSerializer.Serialize(beat);
        var message = $"BEAT {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        await ReceiveResponse("+OK");
    }

    public async Task HelloAsync()
    {
        var hello = new Hello(_workerHostName, _version, _workerId);
        var json = JsonSerializer.Serialize(hello);
        var message = $"HELLO {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        var response = await ReceiveResponse("+OK");
        response.Switch(
            _ => { },
            _ => throw new Exception($"HELLO failed: {response}"));
    }

    public async Task<Job?> FetchJobAsync(string? queueName = null)
    {
        queueName ??= "default";
        var message = $"FETCH {queueName}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        var response = await ReceiveJobResponse();
        return response.Match<Job?>(
            job => job,
            none => null,
            error => throw new Exception($"HELLO failed: {response}"));
    }

    public async Task PushJobAsync(Job job)
    {
        var json = JsonSerializer.Serialize(job);
        var message = $"PUSH {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        var response = await ReceiveResponse("+OK");
        response.Switch(
            _ => { },
            _ => throw new Exception($"PUSH failed: {response}"));
    }

    public async Task<OneOf<Success, Error<string>>> AckJobAsync(string jobId)
    {
        var jid = new {jid = jobId};
        var json = JsonSerializer.Serialize(jid);
        var message = $"ACK {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        return await ReceiveResponse("+OK");
    }

    public async Task<OneOf<Success, Error<string>>> FailJobAsync(string jobId, Exception e)
    {
        var jid = new {jid = jobId, errtype = e.GetType().ToString(), message = $"{e.Message}\n{e.StackTrace}"};
        var json = JsonSerializer.Serialize(jid);
        var message = $"FAIL {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        return await ReceiveResponse("+OK");
    }

    // public async Task<string> InfoAsync(Socket client)
    // {
    //     var message = $"INFO \r\n";
    //     var messageBytes = Encoding.UTF8.GetBytes(message);
    //     await client.SendAsync(messageBytes, SocketFlags.None);
    //     
    //     var response = await ReceiveResponse();
    //     
    //     if (!response.StartsWith("+OK"))
    //     {
    //         throw new Exception($"INFO failed: {response}");
    //     }
    //
    //     return response;
    // }

    private async Task<OneOf<Success, Error<string>>> ReceiveResponse(string expectedResponse)
    {
        var retries = 0;
        var response = "";
        while (retries < 5)
        {
            var buffer = new byte[1024 * 8];
            var received = await _socket.ReceiveAsync(buffer, SocketFlags.None);
            if (received == 0)
            {
                await Task.Delay(50);
                retries++;
                continue;
            }

            response = Encoding.UTF8.GetString(buffer, 0, received);
            if (response.Contains(expectedResponse))
                return new Success();
            if(response.Contains("-ERR Job not found"))
                return new Error<string>("Job not found.");
            await Task.Delay(50);
            retries++;
        }

        await _socket.DisconnectAsync(true);
        await ConnectAsync();
        await HelloAsync();

        _logger?.LogError(
            "ReceiveResponse failed. Tried to receive response {retries} times, but failed. Last response: '{response}'",
            retries, response);
        return new Error<string>("ReceiveResponse failed.");
    }

    private async Task<OneOf<Job, None, Error>> ReceiveJobResponse()
    {
        var retries = 0;
        var response = "";
        var buffer = new byte[1024 * 8];
        var expectedJsonLength = 0;

        while (retries < 10)
        {
            var received = await _socket.ReceiveAsync(buffer, SocketFlags.None);

            if (received == 0)
            {
                await Task.Delay(25);
                retries++;
                continue;
            }

            response = Encoding.UTF8.GetString(buffer, 0, received);

            if (response.StartsWith("$-1"))
                return new None();

            var parts = response.Split('\n');
            var jsonLength = parts[0]
                .Replace("$", "")
                .Replace(" ", "")
                .Replace("\r", "");

            int.TryParse(jsonLength, out expectedJsonLength);

            var byteLength = Encoding.UTF8.GetBytes(response).Length;
            
            if (byteLength >= expectedJsonLength)
            {
                try
                {
                    var job = parts.Length > 1 && parts[1].Contains('{') 
                        ? JsonSerializer.Deserialize<Job>(parts[1]) 
                        : JsonSerializer.Deserialize<Job>(response);
                    if (job != null)
                    {
                        return job;
                    }
                }
                catch (Exception)
                {
                    //ignore, will be retried next time
                    //_logger?.LogError(e, "Failed to deserialize job. Response: '{response}'. Parts: '{parts}'", response, string.Join(", ", parts));
                }
            }

            await Task.Delay(25);
            retries++;
        }

        _logger?.LogError(
            "ReceiveJobResponse failed. Tried to receive response {retries} times, but failed. Response: '{response}'",
            retries, response);
        return new Error();
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }

    private record Hello(
        [property: JsonPropertyName("hostname")]
        string HostName,
        [property: JsonPropertyName("v")] int Version,
        [property: JsonPropertyName("wid")] string? WorkerId,
        [property: JsonPropertyName("labels")] string[]? Labels = null,
        [property: JsonPropertyName("pid")] int? ProcessId = null,
        [property: JsonPropertyName("pwdhash")]
        string? PasswordHash = null);

    public record Job(
        [property: JsonPropertyName("jid")] string JobId,
        [property: JsonPropertyName("queue")] string Queue,
        [property: JsonPropertyName("jobtype")]
        string JobType,
        [property: JsonPropertyName("args")] string[] Arguments,
        [property: JsonPropertyName("reserve_for")]
        int ReserveForSeconds = 60,
        [property: JsonPropertyName("retry")] int RetryAmount = -1,
        [property: JsonPropertyName("created_at")]
        DateTime? CreatedAt = null,
        [property: JsonPropertyName("enqueued_at")]
        DateTime? EnqueuedAt = null,
        [property: JsonPropertyName("failure")]
        Failure? Failure = null);

    public record Failure(
        [property: JsonPropertyName("retry_count")]
        int RetryCount, 
        [property: JsonPropertyName("remaining")]
        int Remaining,
        [property: JsonPropertyName("failed_at")]
        DateTime FailedAt,
        [property: JsonPropertyName("message")]
        string Message,
        [property: JsonPropertyName("error_type")]
        string ErrorType
    );
}
