using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Neleus.DependencyInjection.Extensions;

namespace FaktoryWorker;

internal class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly JobsState _jobsState;
    private static readonly Dictionary<string, IJobConsumer> _consumers = new();
    private readonly WorkerOptions _options;
    private DateTime _timeSinceLastHeartbeat = DateTime.UtcNow;
    private FaktoryClient _client;

    public Worker(ILogger<Worker> logger,
        JobsState jobsState, 
        IOptions<WorkerOptions> options, 
        IServiceByNameFactory<IJobConsumer> factory)
    {
        _logger = logger;
        _jobsState = jobsState;
        _options = options.Value;

        SetupConsumers(factory);
        _client = new FaktoryClient(
            _options.Host, _options.Port, 
            _options.WorkerHostName, _options.FaktoryVersion, _options.WorkerId, 
            _logger);
    }

    private static void SetupConsumers(IServiceByNameFactory<IJobConsumer> factory)
    {
        foreach (var name in factory.GetNames())
        {
            var consumer = factory.GetByName(name);
            _consumers.Add(name, consumer);
        }

        if (_consumers.Count == 0)
            throw new Exception(
                "No consumers were registered. Please register consumers using the AddJobConsumer & Build methods.");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker is stopping.");
        await base.StopAsync(cancellationToken);
        
        var jobsLeft = _jobsState.JobsStarted.Count;
        if (jobsLeft > 0)
        {
            _logger.LogInformation(
                "{count} jobs still in progress. Waiting {seconds} seconds to complete.",
                _jobsState.JobsStarted.Count, _options.ShutdownTimeoutSeconds);
            await Task.Delay(_options.ShutdownTimeoutSeconds * 1000, cancellationToken);
        }
        await AckOrFailJobs(_client);
        var jobsNotCompleted = _jobsState.JobsStarted.Keys.ToList();
        if(jobsNotCompleted.Count > 0)
            _logger.LogWarning("{count} jobs were not completed. JobIds: [{jobIds}]", 
                jobsNotCompleted.Count, 
                string.Join(',', jobsNotCompleted));
        if(jobsNotCompleted.Count == 0 && jobsLeft > 0)
        {
            _logger.LogInformation("All {count} in progress jobs completed successfully.", jobsLeft);
        }
        
        await _client.DisposeAsync();

        _logger.LogInformation("Worker has stopped.");
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker is starting.");
        _logger.LogInformation("WorkerId: {WorkerId}", _options.WorkerId);
        _logger.LogInformation("Worker is connecting to Faktory.");
        await _client.ConnectAsync();
        await _client.HelloAsync();
        _logger.LogInformation("Worker connected successfully.");
        _logger.LogInformation("Working...");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            await AckOrFailJobs(_client);
            
            var potentiallyHasMoreJobs = _consumers
                .Select(x => new {Key = x.Key, Value = true})
                .ToDictionary(x => x.Key, x => x.Value);
            while (potentiallyHasMoreJobs.Any(x => x.Value == true))
            {
                if (_jobsState.JobsStarted.Count >= _options.ParallelJobs) continue;
                await HeartbeatAsync(_client);
                await AckOrFailJobs(_client);

                foreach (var queue in potentiallyHasMoreJobs.Where(x => x.Value == true).Select(x => x.Key))
                {
                    var job = await _client.FetchJobAsync(queue);

                    if (job != null)
                    {
                        if (!_jobsState.JobsStarted.TryAdd(job.JobId, job))
                            throw new Exception("Key (JobId) already exists. JobIds must be unique.");

                        ThreadPool.QueueUserWorkItem(async _ => await HandleJob(job));
                    }
                    else
                    {
                        potentiallyHasMoreJobs[queue] = false;
                    }

                    await Task.Delay(10, cancellationToken);
                }

                await Task.Delay(10, cancellationToken);
            }

            await HeartbeatAsync(_client);
            await Task.Delay(_options.PollingFrequencySeconds, cancellationToken);
        }
    }
    
    private async Task HeartbeatAsync(FaktoryClient client)
    {
        if (DateTime.UtcNow - _timeSinceLastHeartbeat > TimeSpan.FromSeconds(10))
        {
            _timeSinceLastHeartbeat = DateTime.UtcNow;
            await client.HeartbeatAsync();
        }
    }

    private async Task AckOrFailJobs(FaktoryClient client)
    {
        while(_jobsState.JobsCompleted.TryDequeue(out var jobId))
        {
            try
            {
                await client.AckJobAsync(jobId);
            }
            catch (JobNotFoundException e)
            {
                //OK
            }
            catch (FaktoryClientException e)
            {
                //retry once
                try
                {
                    await client.AckJobAsync(jobId);
                }
                catch (FaktoryClientException exception)
                {
                    //Throw it back on the queue
                    _jobsState.JobsCompleted.Enqueue(jobId);
                }
            }
        }

        while(_jobsState.JobsFailed.TryDequeue(out var failedJob))
        {
            try
            {
                await client.FailJobAsync(failedJob.JobId, failedJob.Exception);
            }
            catch (JobNotFoundException e)
            {
                //OK
            }
            catch (FaktoryClientException e)
            {
                //retry once
                try
                {
                    await client.FailJobAsync(failedJob.JobId, failedJob.Exception);
                }
                catch (FaktoryClientException exception)
                {
                    //Throw it back on the queue
                    _jobsState.JobsFailed.Enqueue(failedJob);
                }
            }
        }
    }

    private async Task HandleJob(FaktoryClient.Job job)
    {
        try
        {
            await _consumers[job.Queue].ConsumeJob(job);
            _jobsState.JobsStarted.Remove(job.JobId, out _);
            _jobsState.JobsCompleted.Enqueue(job.JobId);
        }
        catch (Exception? e)
        {
            _logger.LogError(e, "Job failed and will be notified to Faktory as FAIL");
            _jobsState.JobsStarted.Remove(job.JobId, out _);
            _jobsState.JobsFailed.Enqueue((job.JobId, e));
        }
    }
}