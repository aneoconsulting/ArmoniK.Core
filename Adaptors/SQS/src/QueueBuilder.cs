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

using Amazon.SQS;

using ArmoniK.Core.Base;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.SQS;

[PublicAPI]
public class QueueBuilder : IDependencyInjectionBuildable
{
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    var sqsOptions = configuration.GetSection(SQS.SettingSection)
                                  .Get<SQS>() ?? throw new InvalidOperationException("Options not found");

    var client = new AmazonSQSClient(new AmazonSQSConfig
                                     {
                                       ServiceURL = sqsOptions.ServiceURL,
                                     });

    serviceCollection.AddSingleton(client);
    serviceCollection.AddSingleton(sqsOptions);
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
  }
}
