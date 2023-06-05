using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FaktoryWorker;

internal class Worker : BackgroundService
{
    private readonly IJobConsumer _jobConsumer;
    private readonly ILogger<Worker> _logger;
    private readonly JobsState _jobsState;
    private readonly WorkerOptions _options;
    private DateTime _timeSinceLastHeartbeat = DateTime.UtcNow;
    private FaktoryClient _client;

    public Worker(ILogger<Worker> logger, JobsState jobsState, IOptions<WorkerOptions> options, IJobConsumer jobConsumer)
    {
        _jobConsumer = jobConsumer;
        _logger = logger;
        _jobsState = jobsState;
        _options = options.Value;
        _client = new FaktoryClient(_options.Host, _options.Port, _logger);
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
        await _client.HelloAsync(_options.WorkerHostName, _options.FaktoryVersion, _options.WorkerId!);
        _logger.LogInformation("Worker connected successfully.");
        _logger.LogInformation("Working...");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            await AckOrFailJobs(_client);

            var potentiallyHasMoreJobs = true;
            while (potentiallyHasMoreJobs)
            {
                if (_jobsState.JobsStarted.Count >= _options.ParallelJobs) continue;
                await HeartbeatAsync(_client, _options.WorkerId!);
                await AckOrFailJobs(_client);
                var job = await _client.FetchJobAsync();

                if (job != null)
                {
                    if (!_jobsState.JobsStarted.TryAdd(job.JobId, job))
                        throw new Exception("Key (JobId) already exists. JobIds must be unique.");

                    ThreadPool.QueueUserWorkItem(async _ => await HandleJob(job));
                }
                else
                {
                    potentiallyHasMoreJobs = false;
                }
                
                await Task.Delay(10, cancellationToken);
            }
            
            await HeartbeatAsync(_client, _options.WorkerId);
            await Task.Delay(_options.PollingFrequencySeconds, cancellationToken);
        }
    }
    
    private async Task HeartbeatAsync(FaktoryClient client, string workerId)
    {
        if (DateTime.UtcNow - _timeSinceLastHeartbeat > TimeSpan.FromSeconds(10))
        {
            _timeSinceLastHeartbeat = DateTime.UtcNow;
            await client.HeartbeatAsync(workerId);
        }
    }

    private async Task AckOrFailJobs(FaktoryClient client)
    {
        while(_jobsState.JobsCompleted.TryDequeue(out var jobId))
        {
            var result = await client.AckJobAsync(jobId);
            result.Switch( 
                _ => {},
                async error => {
                    if(error.Value == "Job not found.") return;
                    var retry = await client.AckJobAsync(jobId);
                    retry.Switch(
                        success => { },
                        error => _jobsState.JobsCompleted.Enqueue(jobId));
                });
        }

        while(_jobsState.JobsFailed.TryDequeue(out var jobId))
        {
            var result = await client.FailJobAsync(jobId.JobId, jobId.Exception);
            result.Switch( 
                _ => {},
                async error => { 
                    if(error.Value == "Job not found.") return;
                    var retry = await client.FailJobAsync(jobId.JobId, jobId.Exception);
                    retry.Switch(
                        success => { },
                        error => _jobsState.JobsFailed.Enqueue(jobId));
                });
        }
    }

    private async Task HandleJob(FaktoryClient.Job job)
    {
        try
        {
            await _jobConsumer.ConsumeJob(job);
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