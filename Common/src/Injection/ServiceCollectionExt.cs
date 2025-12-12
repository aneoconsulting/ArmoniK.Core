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
using System.Reflection;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Calzolari.Grpc.AspNetCore.Validation;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using ConfigurationExt = ArmoniK.Core.Utils.ConfigurationExt;
using CreateSessionRequestValidator = ArmoniK.Core.Common.gRPC.Validators.SessionsService.CreateSessionRequestValidator;

namespace ArmoniK.Core.Common.Injection;

/// <summary>
///   Extends the functionality of the <see cref="IServiceCollection" />
/// </summary>
public static class ServiceCollectionExt
{
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

    var workerChannelOptions = computePlanComponent.GetRequiredSection(ComputePlane.WorkerChannelSection);
    var agentChannelOptions  = computePlanComponent.GetRequiredSection(ComputePlane.AgentChannelSection);

    var parsedComputePlane = new ComputePlane
                             {
                               WorkerChannel = new GrpcChannel
                                               {
                                                 Address = ConfigurationExt.GetRequiredValue<string>(workerChannelOptions,
                                                                                                     "Address"),
                                                 SocketType = workerChannelOptions.GetValue("SocketType",
                                                                                            GrpcSocketType.UnixDomainSocket),
                                                 KeepAlivePingTimeOut = workerChannelOptions.GetTimeSpanOrDefault("KeepAlivePingTimeOut",
                                                                                                                  TimeSpan.FromSeconds(20)),
                                                 KeepAliveTimeOut = workerChannelOptions.GetTimeSpanOrDefault("KeepAliveTimeOut",
                                                                                                              TimeSpan.FromSeconds(130)),
                                               },
                               AgentChannel = new GrpcChannel
                                              {
                                                Address = ConfigurationExt.GetRequiredValue<string>(agentChannelOptions,
                                                                                                    "Address"),
                                                SocketType = agentChannelOptions.GetValue("SocketType",
                                                                                          GrpcSocketType.UnixDomainSocket),
                                                KeepAlivePingTimeOut = agentChannelOptions.GetTimeSpanOrDefault("KeepAlivePingTimeOut",
                                                                                                                TimeSpan.FromSeconds(20)),
                                                KeepAliveTimeOut = agentChannelOptions.GetTimeSpanOrDefault("KeepAliveTimeOut",
                                                                                                            TimeSpan.FromSeconds(130)),
                                              },
                               MessageBatchSize = computePlanComponent.GetValue("MessageBatchSize",
                                                                                1),
                               AbortAfter = computePlanComponent.GetValue("AbortAfter",
                                                                          TimeSpan.Zero),
                             };

    services.AddSingleton(parsedComputePlane)
            .AddSingleton(parsedComputePlane.WorkerChannel)
            .AddOption<Components>(configuration,
                                   Components.SettingSection)
            .AddOption<InitWorker>(configuration,
                                   InitWorker.SettingSection)
            .AddSingleton<GrpcChannelProvider>()
            .AddSingletonWithHealthCheck<IWorkerStreamHandler, WorkerStreamHandler>(nameof(IWorkerStreamHandler));

    return services;
  }


  /// <summary>
  ///   Dynamically load the services from the specified storage that implements <see cref="IDependencyInjectionBuildable" />
  ///   and add them to the service collection
  /// </summary>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="configuration">Configuration manager to retrieve adapter settings</param>
  /// <param name="storage">Storage identifier for the adapter settings</param>
  /// <param name="logger">Logger instance for logging information</param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  /// <exception cref="InvalidOperationException">
  ///   Thrown when the adapter absolute path or class name is null or empty, or when the provided type does not implement
  ///   <see cref="IDependencyInjectionBuildable" /> interface, or when the type cannot be instantiated.
  /// </exception>
  public static IServiceCollection AddAdapter(this IServiceCollection services,
                                              ConfigurationManager    configuration,
                                              string                  storage,
                                              ILogger                 logger)
  {
    var settings = ConfigurationExt.GetRequiredValue<AdapterSettings>(configuration,
                                                                      $"{Components.SettingSection}:{storage}");

    logger.LogInformation("{storage} settings for loading adapter {@settings}",
                          storage,
                          settings);
    logger.LogDebug("{path}",
                    settings.AdapterAbsolutePath);
    logger.LogDebug("{class}",
                    settings.ClassName);

    if (string.IsNullOrEmpty(settings.AdapterAbsolutePath))
    {
      throw new InvalidOperationException($"{nameof(settings.AdapterAbsolutePath)} should not be null or empty.");
    }

    if (string.IsNullOrEmpty(settings.ClassName))
    {
      throw new InvalidOperationException($"{nameof(settings.ClassName)} should not be null or empty.");
    }

    var assembly = Assembly.LoadFrom(settings.AdapterAbsolutePath);

    logger.LogInformation("Loaded assembly {assemblyName}",
                          assembly.FullName);

    var type = assembly.GetType(settings.ClassName,
                                true,
                                true);

    logger.LogDebug("class loaded {type}",
                    type);

    if (!typeof(IDependencyInjectionBuildable).IsAssignableFrom(type))
    {
      throw new InvalidOperationException($"Provided Type does not implement {nameof(IDependencyInjectionBuildable)} interface.");
    }

    if (Activator.CreateInstance(type) is not IDependencyInjectionBuildable builder)
    {
      throw new InvalidOperationException("Cannot instantiate loaded type.");
    }

    builder.Build(services,
                  configuration,
                  logger);

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
               .AddValidator<gRPC.Validators.CreateSessionRequestValidator>()
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
               .AddValidator<EventSubscriptionRequestValidator>()
               .AddValidator<SubmitTasksRequestValidator>()
               .AddGrpcValidation();

  /// <summary>
  ///   Add singleton for <see cref="ExceptionManager" />
  /// </summary>
  /// <param name="services">Collection of service descriptors</param>
  /// <param name="optionsFactory">
  ///   Function to create <see cref="ExceptionManager.Options" /> from
  ///   <see cref="ServiceProvider" />
  /// </param>
  /// <returns>
  ///   The updated collection of service descriptors
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddExceptionManager(this IServiceCollection                           services,
                                                       Func<IServiceProvider, ExceptionManager.Options>? optionsFactory = null)
    => services.AddSingleton<ExceptionManager>()
               .AddSingleton(optionsFactory ?? (_ => new ExceptionManager.Options()))
               .AddSingleton<IHostLifetime>(sp => sp.GetRequiredService<ExceptionManager>());
}
