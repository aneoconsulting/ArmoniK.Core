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
using System.Reflection;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Utils;

using Calzolari.Grpc.AspNetCore.Validation;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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

    var computePlanOptions = computePlanComponent.Get<ComputePlane>();

    if (computePlanOptions == null)
    {
      return services;
    }

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

  public static IServiceCollection AddQueue(this IServiceCollection services,
                                            ConfigurationManager    configuration,
                                            ILogger                 logger)
  {
    var queueSettings = configuration.GetRequiredValue<AdapterSettings>($"{Components.SettingSection}:{nameof(Components.QueueStorage)}");

    logger.LogInformation("Queue settings for loading adapter {@queueSettings}",
                          queueSettings);
    logger.LogDebug("{path}",
                    queueSettings.AdapterAbsolutePath);
    logger.LogDebug("{class}",
                    queueSettings.ClassName);

    if (string.IsNullOrEmpty(queueSettings.AdapterAbsolutePath))
    {
      throw new InvalidOperationException($"{nameof(queueSettings.AdapterAbsolutePath)} should not be null or empty.");
    }

    if (string.IsNullOrEmpty(queueSettings.ClassName))
    {
      throw new InvalidOperationException($"{nameof(queueSettings.ClassName)} should not be null or empty.");
    }

    var ctx      = new AdapterLoadContext(queueSettings.AdapterAbsolutePath);
    var assembly = ctx.LoadFromAssemblyName(AssemblyName.GetAssemblyName(queueSettings.AdapterAbsolutePath));
    logger.LogInformation("Loaded assembly {assemblyName}",
                          assembly.FullName);

    var type = assembly.GetType(queueSettings.ClassName,
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
