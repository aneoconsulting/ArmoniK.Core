// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Registers the ArmoniK Broker queue in the dependency injection container.
/// </summary>
[PublicAPI]
public class QueueBuilder : IDependencyInjectionBuildable
{
  /// <inheritdoc />
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    var options = configuration.GetSection(Broker.SettingSection)
                               .Get<Broker>() ?? throw new InvalidOperationException("Options not found");
    if (string.IsNullOrEmpty(options.Endpoint))
    {
      throw new InvalidOperationException($"{Broker.SettingSection}:{nameof(Broker.Endpoint)} is required");
    }

    serviceCollection.AddSingleton(options);
    serviceCollection.AddSingleton(sp => new BrokerClient(options,
                                                             sp.GetRequiredService<ILogger<BrokerClient>>()));
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
  }
}
