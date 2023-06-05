namespace FaktoryWorker;

public interface IJobConsumer
{
    Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default);
}