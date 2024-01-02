// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddMongoComponents(this IServiceCollection services,
                                                      ConfigurationManager    configuration,
                                                      ILogger                 logger)
  {
    logger.LogInformation("Configure MongoDB client");

    var components = configuration.GetSection(Components.SettingSection);


    if (components["TableStorage"] == "ArmoniK.Adapters.Memory.TableStorage")
    {
      services.AddTransient<ITaskTable, TaskTable>()
              .AddTransient<ISessionTable, SessionTable>()
              .AddTransient<IResultTable, ResultTable>();
    }

    return services;
  }
}
