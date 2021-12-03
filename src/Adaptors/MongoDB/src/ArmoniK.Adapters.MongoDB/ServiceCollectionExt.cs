// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection.Options;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Driver;

using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace ArmoniK.Adapters.MongoDB
{
  public static class ServiceCollectionExt
  {
    [PublicAPI]
    public static IServiceCollection AddMongoComponents(
      this IServiceCollection serviceCollection,
      IConfiguration          configuration
    )
    {
      serviceCollection.Configure<Options.MongoDB>(configuration.GetSection(Options.MongoDB.SettingSection))
                       .Configure<Options.QueueStorage>(configuration.GetSection(Options.QueueStorage.SettingSection))
                       .Configure<Options.TableStorage>(configuration.GetSection(Options.TableStorage.SettingSection))
                       .Configure<Options.LeaseProvider>(configuration.GetSection(Options.LeaseProvider.SettingSection))
                       .Configure<Options.ObjectStorage>(configuration.GetSection(Options.ObjectStorage.SettingSection))
                       .AddTransient<IMongoClient>
                       (provider =>
                       {
                         var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();
                         return new MongoClient
                           (options.Value.ConnectionString);
                       })
                       .AddTransient
                       (provider =>
                       {
                         var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();
                         return provider.GetRequiredService<IMongoClient>().GetDatabase(options.Value.DatabaseName);
                       })
                       .AddSingleton<SessionProvider>()
                       .AddSingleton(typeof(MongoCollectionProvider<>))
                       .AddTransient<TableStorage>()
                       .AddTransient<LeaseProvider>()
                       .AddTransient<ObjectStorage>()
                       .AddTransient<KeyValueStorage<TaskId, ComputeReply>>()
                       .AddTransient<KeyValueStorage<TaskId, Payload>>()
                       .AddTransient<QueueStorage>();

      var components = configuration.GetSection(Components.SettingSection);

      if (components["TableStorage"] == "ArmoniK.Adapters.MongoDB.TableStorage")
        serviceCollection.AddTransient<ITableStorage, TableStorage>();

      if (components["QueueStorage"] == "ArmoniK.Adapters.MongoDB.QueueStorage")
        serviceCollection.AddTransient<IQueueStorage, QueueStorage>();

      if (components["ObjectStorage"] == "ArmoniK.Adapters.MongoDB.ObjectStorage")
        serviceCollection.AddTransient<IObjectStorage, ObjectStorage>();

      if (components["LeaseProvider"] == "ArmoniK.Adapters.MongoDB.LeaseProvider")
        serviceCollection.AddTransient<ILeaseProvider, LeaseProvider>();

      return serviceCollection;
    }
  }
}