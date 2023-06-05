using FaktoryWorker;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
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
        
        services.AddSingleton<IJobConsumer, ExampleJobConsumer>();
        
        services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
    })
    .Build();

await host.RunAsync();