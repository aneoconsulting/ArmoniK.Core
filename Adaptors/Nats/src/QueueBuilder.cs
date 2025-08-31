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

namespace ArmoniK.Core.Adapters.Nats;

/// <summary>
///   Build object for Nats Jetstream Adapter through dependancy.
/// </summary>
public class QueueBuilder : IDependencyInjectionBuildable
{
  /// <inheritdoc />
  /// <remarks>
  ///   Registers all NATS-related services into the dependency injection container.
  ///   - Loads NATS options from configuration (throws if missing).
  ///   - Registers the NATS options instance as a singleton.
  ///   - Creates and registers a JetStream context (INatsJSContext) from a NatsClient.
  ///   - Registers queue storage implementations: PullQueueStorage and PushQueueStorage.
  /// </remarks>
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    var natsOptions = configuration.GetSection(Nats.SettingSection)
                                   .Get<Nats>() ?? throw new InvalidOperationException("Options not found");
    Console.WriteLine(natsOptions.Url);
    serviceCollection.AddSingleton(natsOptions);
    serviceCollection.AddSingleton<INatsJSContext>(sp =>
                                                   {
                                                     var nc = new NatsClient(natsOptions.Url);
                                                     return nc.CreateJetStreamContext();
                                                   });

    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
  }
}
