# FaktoryWorker
[![build-and-test](https://github.com/tpod/FaktoryWorker/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/tpod/FaktoryWorker/actions/workflows/build-and-test.yml)
[![publish-nuget](https://github.com/tpod/FaktoryWorker/actions/workflows/publish-nuget.yml/badge.svg)](https://github.com/tpod/FaktoryWorker/actions/workflows/publish-nuget.yml)

[![NuGet Release](https://img.shields.io/nuget/v/FaktoryWorker)](https://www.nuget.org/packages/FaktoryWorker)
![NuGet Release](https://img.shields.io/badge/dotnet%20version-net6.0%20%7C%20net7.0-blue)

A simple .NET worker and client for [Faktory Job Server](https://github.com/contribsys/faktory). For more information & documentation about Faktory - go to [Faktory Job Server](https://github.com/contribsys/faktory).


# Quick Start

> `dotnet add package FaktoryWorker`

## Client
Using the `FaktoryClient` you can connect to the Faktory Server and publish jobs like so:

```csharp
await using var faktoryClient = new FaktoryClient("127.0.0.1", 7419, "testworker", 2, Guid.NewGuid().ToString());
await faktoryClient.ConnectAsync();
await faktoryClient.HelloAsync();

var job = new FaktoryClient.Job(Guid.NewGuid().ToString(),"default", "SendEmail", new []{"test"});
await faktoryClient.PushJobAsync(job);
```

Please note that ConnectAsync and HelloAsync need to be done in that order before other commands are executed.
See [FaktoryClientTests.cs](https://github.com/tpod/FaktoryWorker/blob/main/Tests/FaktoryClientTests.cs) for a full example. 


## Worker
In a HostBuilder, you can use the `AddFaktoryWorker` extension method to configure dependency injection for the worker. 
The worker will then start as a `BackgroundService` and poll for jobs every `PollingFrequencySeconds` until the app is stopped. When stopped, if there are any jobs in progress, the worker will wait for the specified `ShutdownTimeoutSeconds` until exiting. 

The worker runs with a single client and socket connection, but is able to process multiple jobs in seperate background threads. Configure `ParalellJobs` to a sensible value according to your specific jobs and hardware, otherwise the worker may eat up too much CPU & Memory. 

```csharp
services.AddFaktoryWorker(options =>
{
    options.Host = "localhost";
    options.Port = 7419;
    options.ParallelJobs = 25;
    options.PollingFrequencySeconds = 1;
    options.ShutdownTimeoutSeconds = 15;
    options.FaktoryVersion = 2;
    options.WorkerHostName = "BackgroundServiceExample";
    options.WorkerId = Guid.NewGuid().ToString();
});
```

Its a good idea to set the Hosts `ShutdownTimeout` to a value larger than the worker's `ShutdownTimeoutSeconds`.
```csharp
services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
```

Create consumers for each Faktory queue you need using the `IJobConsumer` interface.
```csharp
public class ExampleAJobConsumer : IJobConsumer
{
    public async Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default)
    {
        //Implement the logic here
    }
}
```

Then configure dependency injection for your consumers and name which queue they should consume from
```csharp
services
    .AddJobConsumer<ExampleAJobConsumer>("default", services)
    .AddJobConsumer<ExampleBJobConsumer>("queue2", services)
    .Build();
```


See [BackgroundServiceExample](https://github.com/tpod/FaktoryWorker/tree/main/BackgroundServiceExample) for a full runnable example. 


# Faktory API

The following have been implemented so far (enough to push jobs and setup a functional worker):\
`HI` (connect socket)\
`HELLO` (handshake/initialize the worker)\
`BEAT` (heartbeat)\
`PUSH` (push a job to server)\
`FETCH` (fetch a job from server)\
`ACK` (notify job was successfully processed)\
`FAIL` (notify job failed)

For full client API support the following are needed:\
`FLUSH`\
`END`\
`PUSHB`\
`QUEUE REMOVE`\
`QUEUE PAUSE`\
`QUEUE RESUME`\
`INFO`

# TODO

- [ ] Reconnecting to Faktory if the connection is lost.  
- [ ] Full API support.


