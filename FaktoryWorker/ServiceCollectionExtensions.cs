using Microsoft.Extensions.DependencyInjection;

namespace FaktoryWorker;

public static class ServiceCollectionExtensions
{
    public static void AddFaktoryWorker(this IServiceCollection services, Action<WorkerOptions> configuration)
    {
        services.Configure(configuration);
        services.AddHostedService<Worker>();
        services.AddSingleton<JobsState>();
    }
}