namespace FaktoryWorker;

/// <summary>
/// The interface that should be implemented by a job consumer.
/// </summary>
public interface IJobConsumer
{
    /// <summary>
    /// This method is called when a job is fetched from the Faktory server.
    /// Here you should implement the logic for the job.
    /// </summary>
    /// <param name="job"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ConsumeJob(FaktoryClient.Job job, CancellationToken cancellationToken = default);
}