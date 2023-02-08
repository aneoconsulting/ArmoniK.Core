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

using System;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Stream.Worker;

using Calzolari.Grpc.AspNetCore.Validation;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Injection;

/// <summary>
///   Extends the functionality of the <see cref="IConfiguration" />
/// </summary>
public static class ConfigurationExt
{
  /// <summary>
  ///   Configure an object with the given configuration.
  /// </summary>
  /// <typeparam name="T">Type of the options class</typeparam>
  /// <param name="configuration">Configurations used to populate the class</param>
  /// <param name="key">Path to the Object in the configuration</param>
  /// <returns>
  ///   The initialized object
  /// </returns>
  /// <exception cref="InvalidOperationException">the <paramref name="key" /> is not found in the configurations.</exception>
  public static T GetRequiredValue<T>(this IConfiguration configuration,
                                      string              key)
    => configuration.GetRequiredSection(key)
                    .Get<T>();

  /// <summary>
  ///   Configure an object with the given configuration.
  ///   If the object is not found in the configuration, a new object in returned.
  /// </summary>
  /// <typeparam name="T">Type of the options class</typeparam>
  /// <param name="configuration">Configurations used to populate the class</param>
  /// <param name="key">Path to the Object in the configuration</param>
  /// <returns>
  ///   The initialized object
  /// </returns>
  public static T GetInitializedValue<T>(this IConfiguration configuration,
                                         string              key)
    where T : new()
    => configuration.GetSection(key)
                    .Get<T>() ?? new T();
}

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

  /// <summary>
  ///   Add the services to create connection to the worker
  /// </summary>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="configuration">Collection of configuration used to configure the added services</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddArmoniKWorkerConnection(this IServiceCollection services,
                                                              IConfiguration          configuration)
  {
    var computePlanComponent = configuration.GetSection(ComputePlane.SettingSection);
    if (!computePlanComponent.Exists())
    {
      return services;
    }

    var computePlanOptions = computePlanComponent.Get<ComputePlane>();

    services.AddSingleton(computePlanOptions)
            .AddSingleton(computePlanOptions.WorkerChannel)
            .AddOption<Components>(configuration,
                                   Components.SettingSection)
            .AddOption<InitWorker>(configuration,
                                   InitWorker.SettingSection)
            .AddSingleton<GrpcChannelProvider>()
            .AddSingletonWithHealthCheck<IWorkerStreamHandler, WorkerStreamHandler>(nameof(IWorkerStreamHandler));

    return services;
  }

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

  /// <summary>
  ///   Add validation services for gRPC Requests
  /// </summary>
  /// <param name="services">Collection of service descriptors</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection ValidateGrpcRequests(this IServiceCollection services)
    => services.AddGrpc(options =>
                        {
                          options.EnableMessageValidation();
                          options.MaxReceiveMessageSize = null;
                        })
               .Services.AddValidator<CreateLargeTaskRequestValidator>()
               .AddValidator<CreateSmallTaskRequestValidator>()
               .AddValidator<CreateSessionRequestValidator>()
               .AddValidator<CancelTasksRequestValidator>()
               .AddValidator<TaskOptionsValidator>()
               .AddValidator<TaskFilterValidator>()
               .AddValidator<SessionFilterValidator>()
               .AddValidator<ListSessionsRequestValidator>()
               .AddValidator<ListTasksRequestValidator>()
               .AddValidator<ListResultsRequestValidator>()
               .AddValidator<ListApplicationsRequestValidator>()
               .AddValidator<ListPartitionsRequestValidator>()
               .AddValidator<SessionsCountTasksByStatusRequestValidator>()
               .AddValidator<ApplicationsCountTasksByStatusRequestValidator>()
               .AddGrpcValidation();
}
