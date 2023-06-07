using Microsoft.Extensions.DependencyInjection;
using Neleus.DependencyInjection.Extensions;

namespace FaktoryWorker;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the worker background service as a HostedService to the service collection.
    /// Configures the worker options.
    /// </summary>
    /// <param name="services"></param>
    /// <param name="configuration"></param>
    public static void AddFaktoryWorker(this IServiceCollection services, Action<WorkerOptions> configuration)
    {
        services.Configure(configuration);
        services.AddHostedService<Worker>();
        services.AddSingleton<JobsState>();
        services.AddByName<IJobConsumer>().Build();
    }
    
    public static ServicesByNameBuilder<IJobConsumer> AddJobConsumer<T>(this IServiceCollection services, string queueName, IServiceCollection servicesCollection) where T : class, IJobConsumer
    {
        services.AddTransient<T>();
        return services
            .AddByName<IJobConsumer>()
            .Add<T>(queueName);
    }
    
    public static ServicesByNameBuilder<IJobConsumer> AddJobConsumer<T>(this ServicesByNameBuilder<IJobConsumer> nameBuilder, string queueName, IServiceCollection services) 
        where T : class, IJobConsumer
    {
        services.AddTransient<T>();
        nameBuilder.Add<T>(queueName);
        return nameBuilder;
    }
}