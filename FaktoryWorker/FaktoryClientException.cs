namespace FaktoryWorker;

public class FaktoryClientException : Exception
{
    public FaktoryClientException(string message) : base(message)
    {
    }
}

public class JobNotFoundException : Exception
{
    public JobNotFoundException(string? message = null) : base(message)
    {
    }
}