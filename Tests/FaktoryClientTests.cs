using DotNet.Testcontainers.Builders;
using FaktoryWorker;
using FluentAssertions;
using Xunit.Abstractions;

namespace Tests;

public class FaktoryClientTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public FaktoryClientTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task HappyFlow()
    {
        var dockerEndpoint = Environment.GetEnvironmentVariable("DOCKER_HOST") ?? "unix:/var/run/docker.sock";
        
        // Start a Faktory Job Server container locally for this test
        var container = new ContainerBuilder()
            .WithDockerEndpoint(dockerEndpoint)
            .WithImage("contribsys/faktory:latest")
            .WithPortBinding(7419, true)
            .WithExposedPort(7419)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(7419))
            .WithStartupCallback((_, ct) => Task.Delay(TimeSpan.FromMilliseconds(500), ct))
            .WithCleanUp(true)
            .Build();
        await container.StartAsync();
        _testOutputHelper.WriteLine($"Faktory server running at {container.Hostname}:{container.GetMappedPublicPort(7419)}");
        
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