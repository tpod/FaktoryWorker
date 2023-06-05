using DotNet.Testcontainers.Builders;
using FaktoryWorker;
using FluentAssertions;

namespace Tests;

public class FaktoryClientTests
{
    [Fact]
    public async Task HappyFlow()
    {
        // Start a Faktory Job Server container locally for this test
        var container = new ContainerBuilder()
            .WithImage("contribsys/faktory:latest")
            .WithPortBinding(7419, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(7419))
            .Build();
        await container.StartAsync();
        
        //Connect to server
        await using var faktoryClient = new FaktoryClient(container.Hostname, container.GetMappedPublicPort(7419));
        await faktoryClient.ConnectAsync();
        var wid = Guid.NewGuid().ToString();
        await faktoryClient.HelloAsync("testworker", 2, wid);
        await faktoryClient.HeartbeatAsync(wid);
        
        //Push a job
        var job = new FaktoryClient.Job(Guid.NewGuid().ToString(),"default", "SendEmail", new []{"test"});
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
}