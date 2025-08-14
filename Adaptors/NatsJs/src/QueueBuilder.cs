// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NATS.Client.JetStream;
using NATS.Net;

// Install NuGet package `NATS.Net`

namespace ArmoniK.Core.Adapters.Nats;

public class QueueBuilder : IDependencyInjectionBuildable
{
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    var natsOptions = configuration.GetSection(Nats.SettingSection)
                                   .Get<Nats>() ?? throw new InvalidOperationException("Options not found");

    serviceCollection.AddSingleton(natsOptions);

    // Access JetStream for managing streams and consumers as well as for
    // publishing and consuming messages to and from the stream.
    // Register connection singleton (shared connection)
    serviceCollection.AddSingleton<INatsJSContext>(sp =>
                                                   {
                                                     var nc = new NatsClient(natsOptions.Url);
                                                     return nc.CreateJetStreamContext();
                                                   });

    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
  }
}
