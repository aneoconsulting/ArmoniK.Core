// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmoniK.Core.Adapters.LocalStorage;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddLocalStorage(this IServiceCollection serviceCollection,
                                                   ConfigurationManager    configuration,
                                                   ILogger                 logger)
  {
    var components = configuration.GetSection(Components.SettingSection);

    if (components["ObjectStorage"] != "ArmoniK.Adapters.LocalStorage.ObjectStorage")
    {
      return serviceCollection;
    }

    serviceCollection.AddOption(configuration,
                                Options.LocalStorage.SettingSection,
                                out Options.LocalStorage storageOptions);

    using var _ = logger.BeginNamedScope("Object Local configuration",
                                         ("Path", storageOptions.Path));

    logger.LogDebug("setup local storage");

    serviceCollection.AddSingletonWithHealthCheck<IObjectStorageFactory>(nameof(IObjectStorageFactory),
                                                                         sp => new ObjectStorageFactory(storageOptions.Path,
                                                                                                        storageOptions.ChunkSize,
                                                                                                        sp.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance));

    return serviceCollection;
  }
}
