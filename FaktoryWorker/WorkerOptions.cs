namespace FaktoryWorker;

public class WorkerOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 7419;
    public int FaktoryVersion { get; set; } = 2;
    public int ShutdownTimeoutSeconds { get; set; } = 15;
    public int PollingFrequencySeconds { get; set; } = 1;
    public int ParallelJobs { get; set; } = 25;
    public string WorkerHostName { get; set; } = "BackgroundServiceExample";
    public string WorkerId { get; set; } = Guid.NewGuid().ToString();
}