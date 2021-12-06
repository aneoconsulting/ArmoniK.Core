// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Adapters.Amqp.Options;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ArmoniK.Adapters.Amqp
{
  public static class ServiceCollectionExt
  {
    [PublicAPI]
    public static IServiceCollection AddAmqp(
      this IServiceCollection serviceCollection,
      IConfiguration          configuration
    )
    {
      serviceCollection.Configure<AmqpOptions>(configuration.GetSection(AmqpOptions.SettingSection))
                       .AddSingleton<SessionProvider>();

      var components = configuration.GetSection(Components.SettingSection);
      
      if (components["QueueStorage"] == "ArmoniK.Adapters.Amqp.QueueStorage")
        serviceCollection.AddSingleton<IQueueStorage, QueueStorage>();

      return serviceCollection;
    }
  }
}