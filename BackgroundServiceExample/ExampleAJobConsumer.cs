namespace FaktoryWorker;

public class ExampleAJobConsumer : IJobConsumer
{
    private readonly ILogger<ExampleAJobConsumer> _logger;

    public ExampleAJobConsumer(ILogger<ExampleAJobConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "'{jobType}' job from '{queue}' queue with id '{jobId}' started working using '{consumer}' consumer.", 
            job.JobType, job.Queue, job.JobId, nameof(ExampleAJobConsumer));
        
        // Simulate work
        await Task.Delay(new Random().Next(1000, 5000), cancellationToken);
        
        //Simulate failure sometimes
        if (new Random().Next(0, 5) == 0)
            throw new NullReferenceException("Something went wrong.");
        
        _logger.LogInformation("'{jobId}' completed.", job.JobId);
    }
}