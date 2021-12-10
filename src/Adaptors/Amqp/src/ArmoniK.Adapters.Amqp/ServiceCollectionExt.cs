// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

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
      serviceCollection.Configure<Options.Amqp>(configuration.GetSection(Options.Amqp.SettingSection))
                       .AddSingleton<SessionProvider>();

      var components = configuration.GetSection(Components.SettingSection);
      
      if (components["QueueStorage"] == "ArmoniK.Adapters.Amqp.QueueStorage")
        serviceCollection.AddSingleton<IQueueStorage, QueueStorage>();

      return serviceCollection;
    }
  }
}