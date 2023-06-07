using FaktoryWorker;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        //Add and configure the FaktoryWorker
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
        
        //Add consumers and name which queue they should consume from
        services
            .AddJobConsumer<ExampleAJobConsumer>("default", services)
            .AddJobConsumer<ExampleBJobConsumer>("queue2", services)
            .Build();

        //Configure the shutdown timeout of the host itself.
        //Should be set to a value higher than the shutdown timeout of the FaktoryWorker. 
        services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
    })
    .Build();

await host.RunAsync();