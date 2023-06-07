namespace FaktoryWorker;

public class WorkerOptions
{
    /// <summary>
    /// Host or IP address of the Faktory server.
    /// Defaults to localhost.
    /// </summary>
    public string Host { get; set; } = "localhost";
    
    /// <summary>
    /// Port of the Faktory server.
    /// Defaults to 7419.
    /// </summary>
    public int Port { get; set; } = 7419;
    
    /// <summary>
    /// Version of the Faktory server.
    /// Defaults to 2.
    /// </summary>
    public int FaktoryVersion { get; set; } = 2;
    
    /// <summary>
    /// The timeout in seconds for the FaktoryWorker to shutdown gracefully.
    /// The worker will stop processing new jobs and try to finish the jobs that are currently in progress within this timeout.
    /// Defaults to 15.
    /// </summary>
    public int ShutdownTimeoutSeconds { get; set; } = 15;
    
    /// <summary>
    /// Polling frequency in seconds. How often the FaktoryWorker will poll the Faktory server for new jobs.
    /// Defaults to 1.
    /// </summary>
    public int PollingFrequencySeconds { get; set; } = 1;
    
    /// <summary>
    /// How many jobs the FaktoryWorker will try to process in parallel.
    /// Warning: the worker will consume at least this many threads and eat up CPU & memory.
    /// Please set to a sensible value according to your specific jobs and hardware.
    /// </summary>
    public int ParallelJobs { get; set; } = 25;
    
    /// <summary>
    /// The name worker. Will show up in the Faktory web UI.
    /// Defaults to "BackgroundService".
    /// </summary>
    public string WorkerHostName { get; set; } = "BackgroundService";
    
    /// <summary>
    /// The id of the worker. Will show up in the Faktory web UI.
    /// Defaults to a random Guid.
    /// </summary>
    public string WorkerId { get; set; } = Guid.NewGuid().ToString();
}