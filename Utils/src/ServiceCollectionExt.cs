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

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Extends the functionality of the <see cref="IServiceCollection" />
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Fill a class with values found in configurations
  /// </summary>
  /// <typeparam name="T">Class to fill</typeparam>
  /// <param name="services">Collection of services</param>
  /// <param name="configuration">Configurations used to populate the class</param>
  /// <param name="key">Path to the Object in the configuration</param>
  /// <returns>
  ///   Input collection of services to chain usages
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddInitializedOption<T>(this IServiceCollection services,
                                                           IConfiguration          configuration,
                                                           string                  key)
    where T : class, new()
    => services.AddSingleton(configuration.GetInitializedValue<T>(key));

  /// <summary>
  ///   Fills in an option class and add it to the service collection
  /// </summary>
  /// <typeparam name="T">Type of option class to add</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="configuration">Collection of configuration used to configure the option class</param>
  /// <param name="key">Key to find the option to fill</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddOption<T>(this IServiceCollection services,
                                                IConfiguration          configuration,
                                                string                  key)
    where T : class
    => services.AddSingleton(configuration.GetRequiredValue<T>(key));

  /// <summary>
  ///   Fills in an option class, add it in the service collection and return the initialized class
  /// </summary>
  /// <typeparam name="T">Type of option class to add</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="configuration">Collection of configuration used to configure the option class</param>
  /// <param name="key">Key to find the option to fill</param>
  /// <param name="option">Represents the filled option class</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddOption<T>(this IServiceCollection services,
                                                IConfiguration          configuration,
                                                string                  key,
                                                out T                   option)
    where T : class
  {
    option = configuration.GetRequiredValue<T>(key);
    return services.AddSingleton(option);
  }
}
