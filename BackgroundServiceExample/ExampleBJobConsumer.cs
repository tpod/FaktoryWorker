namespace FaktoryWorker;

public class ExampleBJobConsumer : IJobConsumer
{
    public async Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default)
    {
        // Do work
        await Task.CompletedTask;
    }
}