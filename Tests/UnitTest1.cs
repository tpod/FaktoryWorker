using FaktoryWorker;

namespace Tests;

public class UnitTest1
{
    [Fact]
    public async Task Test2()
    {
        await using var faktoryClient = new FaktoryClient("localhost", 7419);
        await faktoryClient.ConnectAsync();
        var wid = Guid.NewGuid().ToString();
        await faktoryClient.HelloAsync("testworker", 2, wid);
        await faktoryClient.HeartbeatAsync(wid);

        for (int i = 0; i < 5000; i++)
        {
            var job = new FaktoryClient.Job(Guid.NewGuid().ToString(),"default", "SendEmail", new []{"test"});
            await faktoryClient.PushJobAsync(job);
        }
        //var fetchedJob = await faktoryClient.FetchJobAsync(job.Queue);
        //await faktoryClient.AckJobAsync(fetchedJob!.JobId);
    }
}