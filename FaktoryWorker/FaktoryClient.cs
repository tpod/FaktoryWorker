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

    /// <summary>
    /// Connects the socket to the Faktory server and expects a "+HI" response.
    /// </summary>
    /// <exception cref="Exception"></exception>
    public async Task ConnectAsync()
    {
        await _socket.ConnectAsync(_ipEndPoint);

        try
        {
            await ReceiveResponse("+HI");
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "Failed to connect to Faktory server");
            throw;
        }
    }

    /// <summary>
    /// Sends a "BEAT" message to the Faktory server. Should be done at least every 15 seconds.
    /// </summary>
    public async Task HeartbeatAsync()
    {
        var beat = new {wid = _workerId};
        var json = JsonSerializer.Serialize(beat);
        var message = $"BEAT {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        try
        {
            await ReceiveResponse("+OK");
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "BEAT failed");
            throw;
        }
    }

    /// <summary>
    /// Sends a "HELLO" message to the Faktory server initializing the WorkerHostName, WorkerId and Version.
    /// This is the handshake that must be done after connecting but before any other commands.
    /// </summary>
    /// <exception cref="Exception"></exception>
    public async Task HelloAsync()
    {
        var hello = new Hello(_workerHostName, _version, _workerId);
        var json = JsonSerializer.Serialize(hello);
        var message = $"HELLO {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        try
        {
            await ReceiveResponse("+OK");
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "HELLO failed");
            throw;
        }
    }

    /// <summary>
    /// Sends a "FETCH" message to the Faktory server to retrieve a job.
    /// Returns null if no job is available.
    /// </summary>
    /// <param name="queueName"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async Task<Job?> FetchJobAsync(string? queueName = null)
    {
        queueName ??= "default";
        var message = $"FETCH {queueName}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);
        
        try
        {
            return await ReceiveJobResponse();
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "FETCH failed");
            throw;
        }
    }

    /// <summary>
    /// Sends a "PUSH" message to the Faktory server with the job to be pushed.
    /// </summary>
    /// <param name="job"></param>
    /// <exception cref="Exception"></exception>
    public async Task PushJobAsync(Job job)
    {
        var json = JsonSerializer.Serialize(job);
        var message = $"PUSH {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        try
        {
            await ReceiveResponse("+OK");
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "PUSH failed");
            throw;
        }
    }

    /// <summary>
    /// Sends a "ACK" message to the Faktory server to acknowledge that the job has been processed successfully.
    /// </summary>
    /// <param name="jobId"></param>
    /// <returns></returns>
    public async Task AckJobAsync(string jobId)
    {
        var jid = new {jid = jobId};
        var json = JsonSerializer.Serialize(jid);
        var message = $"ACK {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        try
        {
            await ReceiveResponse("+OK");
        }
        catch (Exception e)
        {
            _logger?.LogError(e, "ACK failed");
            throw;
        }
    }

    /// <summary>
    /// Sends a "FAIL" message to the Faktory server to indicate that the job has failed.
    /// </summary>
    /// <param name="jobId"></param>
    /// <param name="e"></param>
    /// <returns></returns>
    public async Task FailJobAsync(string jobId, Exception e)
    {
        var jid = new {jid = jobId, errtype = e.GetType().ToString(), message = $"{e.Message}\n{e.StackTrace}"};
        var json = JsonSerializer.Serialize(jid);
        var message = $"FAIL {json}\r\n";
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _socket.SendAsync(messageBytes, SocketFlags.None);

        try
        {
            await ReceiveResponse("+OK");
        }
        catch (Exception exception)
        {
            _logger?.LogError(exception, "FAIL failed");
            throw;
        }
    }

    private async Task ReceiveResponse(string expectedResponse)
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
                return;
            if (response.Contains("-ERR Job not found"))
                throw new JobNotFoundException();
            await Task.Delay(50);
            retries++;
        }

        _logger?.LogError(
            "ReceiveResponse failed. Tried to receive response {retries} times, but failed. Last response: '{response}'",
            retries, response);
        throw new FaktoryClientException("ReceiveResponse failed.");
    }

    private async Task<Job?> ReceiveJobResponse()
    {
        var retries = 0;
        var response = "";
        var buffer = new byte[1024 * 8];
        var expectedJsonLength = 0;

        while (retries < 5)
        {
            var received = await _socket.ReceiveAsync(buffer, SocketFlags.None);

            if (received == 0)
            {
                await Task.Delay(50);
                retries++;
                continue;
            }

            response = Encoding.UTF8.GetString(buffer, 0, received);

            if (response.StartsWith("$-1"))
                return null;

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
        throw new FaktoryClientException("ReceiveJobResponse failed.");
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

    /// <summary>
    /// The job object that can either be pushed to the server or retrieved from the server.
    /// </summary>
    /// <param name="JobId"></param>
    /// <param name="Queue"></param>
    /// <param name="JobType"></param>
    /// <param name="Arguments"></param>
    /// <param name="ReserveForSeconds"></param>
    /// <param name="RetryAmount"></param>
    /// <param name="CreatedAt"></param>
    /// <param name="EnqueuedAt"></param>
    /// <param name="Failure"></param>
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

    /// <summary>
    /// If a job has failed before, this object will be returned from the server.
    /// </summary>
    /// <param name="RetryCount"></param>
    /// <param name="Remaining"></param>
    /// <param name="FailedAt"></param>
    /// <param name="Message"></param>
    /// <param name="ErrorType"></param>
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