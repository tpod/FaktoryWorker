using FaktoryWorker;
using FluentAssertions;

namespace Tests;

public class FaktoryClientTests
{
    [Fact]
    public async Task HappyFlow_Using_StandAlone_FaktoryServer()
    {
        var wid = Guid.NewGuid().ToString();
        await using var faktoryClient = new FaktoryClient("127.0.0.1", 7419, "testworker", 2, wid);
        await faktoryClient.ConnectAsync();
        await faktoryClient.HelloAsync();
        await faktoryClient.HeartbeatAsync();
        
        //Push a job
        var job = new FaktoryClient.Job(Guid.NewGuid().ToString(),"testqueue", "SendEmail", new []{"test"});
        await faktoryClient.PushJobAsync(job);
        
        //Fetch the job
        var fetchedJob = await faktoryClient.FetchJobAsync(job.Queue);
        fetchedJob.Should().NotBeNull();
        fetchedJob!.JobId.Should().Be(job.JobId);
        fetchedJob.Arguments.First().Should().Be("test");
        fetchedJob.JobType.Should().Be("SendEmail");
        
        //Acknowledge the job is done
        await faktoryClient.AckJobAsync(fetchedJob!.JobId);
    }

    [Fact]
    public async Task SeedSomeJobs()
    {
        var wid = Guid.NewGuid().ToString();
        await using var faktoryClient = new FaktoryClient("127.0.0.1", 7419, "testworker", 2, wid);
        await faktoryClient.ConnectAsync();
        await faktoryClient.HelloAsync();

        for(var i = 0; i < 10; i++)
        {
            var job = new FaktoryClient.Job(Guid.NewGuid().ToString(),"default", "SendEmail", new []{"test"});
            await faktoryClient.PushJobAsync(job);
        
            var job2 = new FaktoryClient.Job(Guid.NewGuid().ToString(),"queue2", "SendSms", new []{"test"});
            await faktoryClient.PushJobAsync(job2);
        }
    }
}