// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddRabbit(this IServiceCollection serviceCollection,
                                             ConfigurationManager    configuration,
                                             ILogger                 logger)
  {
    logger.LogInformation("Configure RabbitMQ client");

    var components = configuration.GetSection(Components.SettingSection);

    if (components["QueueStorage"] == "ArmoniK.Adapters.RabbitMQ.QueueStorage")
    {
      serviceCollection.AddOption(configuration,
                                  Amqp.SettingSection,
                                  out Amqp amqpOptions);
      using var _ = logger.BeginNamedScope("AMQP configuration",
                                           ("host", amqpOptions.Host),
                                           ("port", amqpOptions.Port));

      serviceCollection.AddSingletonWithHealthCheck<IConnectionRabbit, ConnectionRabbit>(nameof(IConnectionRabbit));
      serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
      serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

      logger.LogInformation("RabbitMQ configuration complete");
    }
    else
    {
      logger.LogInformation("Nothing to configure");
    }

    return serviceCollection;
  }
}
