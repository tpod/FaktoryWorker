namespace FaktoryWorker;

public class ExampleJobConsumer : IJobConsumer
{
    private readonly ILogger<ExampleJobConsumer> _logger;

    public ExampleJobConsumer(ILogger<ExampleJobConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("{jobType} job with id {jobId} started.", job.JobType, job.JobId);
        
        var jobType = Enum.Parse<JobType>(job.JobType);
        switch (jobType)
        {
            case JobType.SendEmail:
                await SendEmail(job, cancellationToken);
                break;
            case JobType.SendSms:
                break;
            case JobType.SendPushNotification:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
        
        _logger.LogInformation("{jobType} job with id {jobId} completed.", job.JobType, job.JobId);
    }

    private async Task SendEmail(FaktoryClient.Job job, CancellationToken cancellationToken)
    {
        // Simulate work
        await Task.Delay(new Random().Next(1000, 5000), cancellationToken);
        
        //Simulate failure sometimes
        if (new Random().Next(0, 5) == 0)
            throw new NullReferenceException("Something went wrong.");
    }

    private enum JobType
    {
        SendEmail,
        SendSms,
        SendPushNotification
    }
}