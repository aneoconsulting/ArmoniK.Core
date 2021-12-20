// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using ArmoniK.Adapters.MongoDB.Options;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;

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
                       .Configure<QueueStorage>(configuration.GetSection(QueueStorage.SettingSection))
                       .Configure<Options.TableStorage>(configuration.GetSection(Options.TableStorage.SettingSection))
                       .Configure<Options.LeaseProvider>(configuration.GetSection(Options.LeaseProvider.SettingSection))
                       .Configure<Options.ObjectStorage>(configuration.GetSection(Options.ObjectStorage.SettingSection))
                       .AddTransient<IMongoClient>
                          (provider =>
                           {
                             var options             = provider.GetRequiredService<IOptions<Options.MongoDB>>();
                             var logger              = provider.GetRequiredService<ILogger<IMongoClient>>();
                             var mongoConnectionUrl  = new MongoUrl(options.Value.ConnectionString);
                             var mongoClientSettings = MongoClientSettings.FromUrl(mongoConnectionUrl);
                             mongoClientSettings.ClusterConfigurator = cb =>
                                                                       {
                                                                         cb.Subscribe<CommandStartedEvent>(e =>
                                                                                                           {
                                                                                                             logger.LogDebug("{CommandName} - {Command}",
                                                                                                                                           e.CommandName,
                                                                                                                                           e.Command.ToJson());
                                                                                                           });
                                                                       };
                             return new MongoClient(mongoClientSettings);
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
                       .AddTransient<LockedQueueStorage>();

      var components = configuration.GetSection(Components.SettingSection);

      if (components["TableStorage"] == "ArmoniK.Adapters.MongoDB.TableStorage")
        serviceCollection.AddTransient<ITableStorage, TableStorage>();

      if (components["QueueStorage"] == "ArmoniK.Adapters.MongoDB.LockedQueueStorage")
        serviceCollection.AddTransient<IQueueStorage, LockedWrapperQueueStorage>()
                         .AddTransient<ILockedQueueStorage, LockedQueueStorage>();

      if (components["ObjectStorage"] == "ArmoniK.Adapters.MongoDB.ObjectStorage")
        serviceCollection.AddTransient<IObjectStorage, ObjectStorage>();

      if (components["LeaseProvider"] == "ArmoniK.Adapters.MongoDB.LeaseProvider")
        serviceCollection.AddTransient<ILeaseProvider, LeaseProvider>();

      return serviceCollection;
    }
  }
}
