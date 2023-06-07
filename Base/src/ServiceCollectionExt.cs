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

using System;

using JetBrains.Annotations;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Base;

/// <summary>
///   Extends the functionality of the <see cref="IServiceCollection" />
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Add a singleton service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="T">Type of the service (interface)</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddSingletonWithHealthCheck<T>(this IServiceCollection services,
                                                                  string                  checkName)
    where T : class, IHealthCheckProvider
  {
    services.AddSingleton<T>();

    services.AddHealthChecks()
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Startup)}",
                                             provider => new HealthCheck(provider.GetRequiredService<T>(),
                                                                         HealthCheckTag.Startup),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Startup),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Liveness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<T>(),
                                                                         HealthCheckTag.Liveness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Liveness),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Readiness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<T>(),
                                                                         HealthCheckTag.Readiness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Readiness),
                                             }));
    return services;
  }

  /// <summary>
  ///   Add health check for a service
  /// </summary>
  /// <param name="services">Service Collection to which the checks should be added</param>
  /// <param name="checkName">Name of the health check</param>
  /// <typeparam name="TService">Service Type to check</typeparam>
  private static void AddHealthCheck<TService>(IServiceCollection services,
                                               string             checkName)
    where TService : class, IHealthCheckProvider
    => services.AddHealthChecks()
               .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Startup)}",
                                                provider => new HealthCheck(provider.GetRequiredService<TService>(),
                                                                            HealthCheckTag.Startup),
                                                HealthStatus.Unhealthy,
                                                new[]
                                                {
                                                  nameof(HealthCheckTag.Startup),
                                                }))
               .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Liveness)}",
                                                provider => new HealthCheck(provider.GetRequiredService<TService>(),
                                                                            HealthCheckTag.Liveness),
                                                HealthStatus.Unhealthy,
                                                new[]
                                                {
                                                  nameof(HealthCheckTag.Liveness),
                                                }))
               .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Readiness)}",
                                                provider => new HealthCheck(provider.GetRequiredService<TService>(),
                                                                            HealthCheckTag.Readiness),
                                                HealthStatus.Unhealthy,
                                                new[]
                                                {
                                                  nameof(HealthCheckTag.Readiness),
                                                }));

  /// <summary>
  ///   Add a singleton service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="TService">Type of the service (interface)</typeparam>
  /// <typeparam name="TImplementation">Implementation class of the service</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddSingletonWithHealthCheck<TService, TImplementation>(this IServiceCollection services,
                                                                                          string                  checkName)
    where TImplementation : class, TService
    where TService : class, IHealthCheckProvider
  {
    services.AddSingleton<TService, TImplementation>();
    AddHealthCheck<TService>(services,
                             checkName);
    return services;
  }

  /// <summary>
  ///   Add a singleton service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="TService">Type of the service (interface)</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <param name="implementationInstance">The instance of the service</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddSingletonWithHealthCheck<TService>(this IServiceCollection services,
                                                                         string                  checkName,
                                                                         TService                implementationInstance)
    where TService : class, IHealthCheckProvider
  {
    services.AddSingleton(implementationInstance);
    AddHealthCheck<TService>(services,
                             checkName);
    return services;
  }

  /// <summary>
  ///   Add a singleton service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="TService">Type of the service (interface)</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <param name="implementationFactory">The factory that creates the service</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddSingletonWithHealthCheck<TService>(this IServiceCollection          services,
                                                                         string                           checkName,
                                                                         Func<IServiceProvider, TService> implementationFactory)
    where TService : class, IHealthCheckProvider
  {
    services.AddSingleton(implementationFactory);
    AddHealthCheck<TService>(services,
                             checkName);
    return services;
  }

  /// <summary>
  ///   Add a transient service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="T">Type of the service (interface)</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddTransientWithHealthCheck<T>(this IServiceCollection services,
                                                                  string                  checkName)
    where T : class, IHealthCheckProvider
  {
    services.AddTransient<T>();
    AddHealthCheck<T>(services,
                      checkName);
    return services;
  }

  /// <summary>
  ///   Add a transient service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="TService">Type of the service (interface)</typeparam>
  /// <typeparam name="TImplementation">Implementation class of the service</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="checkName">Name for the health check</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddTransientWithHealthCheck<TService, TImplementation>(this IServiceCollection services,
                                                                                          string                  checkName)
    where TImplementation : class, TService
    where TService : class, IHealthCheckProvider
  {
    services.AddTransient<TService, TImplementation>();
    AddHealthCheck<TService>(services,
                             checkName);
    return services;
  }

  /// <summary>
  ///   Add a transient service of the specified type with health check capabilities
  /// </summary>
  /// <typeparam name="TService">Type of the service</typeparam>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="implementationFactory">Factory to create service</param>
  /// <param name="checkName">Name for the health check</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddTransientWithHealthCheck<TService>(this IServiceCollection          services,
                                                                         Func<IServiceProvider, TService> implementationFactory,
                                                                         string                           checkName)
    where TService : class, IHealthCheckProvider
  {
    services.AddTransient(implementationFactory);
    AddHealthCheck<TService>(services,
                             checkName);
    return services;
  }
}
