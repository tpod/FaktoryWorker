using System.Collections.Concurrent;

namespace FaktoryWorker;

internal class JobsState
{
    public ConcurrentDictionary<string, FaktoryClient.Job> JobsStarted { get; set; } = new();
    public ConcurrentQueue<string> JobsCompleted { get; set; } = new();
    public ConcurrentQueue<(string JobId, Exception Exception)> JobsFailed { get; set; } = new();
}