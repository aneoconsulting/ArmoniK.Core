using System;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Injection
{
  public static class IServiceCollectionExt
  {
    public static IServiceCollection AddArmoniKCore(this IServiceCollection services,
                                                    IConfiguration          configuration)
      => services.Configure<ComputePlan>(configuration.GetSection(ComputePlan.SettingSection))
                 .Configure<GrpcChannel>(configuration.GetSection(GrpcChannel.SettingSection))
                 .Configure<Components>(configuration.GetSection(Components.SettingSection))
                 .AddSingleton<GrpcChannelProvider>()
                 .AddSingleton(typeof(KeyValueStorage<,>));
  }

  public abstract class ProviderBase<T>
  {
    private T object_;
    private readonly Func<Task<T>> builder_;

    protected ProviderBase(Func<Task<T>> builder) => builder_ = builder;

    public async ValueTask<T> GetAsync()
    {
      if(object_ is null)
      {
        Task<T> task;
        lock(this)
        {
          task = object_ is null ? builder_() : Task.FromResult(object_);
        }
        object_ = await task;
      }
      return object_;
    }
  }
}
