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

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;
using ArmoniK.Core.Utils.Uuid;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

using Submitter = ArmoniK.Core.Common.Injection.Options.Submitter;

namespace ArmoniK.Core.Common.Injection;

/// <summary>
///   Registration of the services of the control plane and of the compute plane, independently of the storage adapters
///   and of the hosting (logging, telemetry, Kestrel endpoints).
/// </summary>
public static class ArmoniKPlanesExt
{
  /// <summary>
  ///   Register the services of the control plane: the gRPC services exposed to clients and their dependencies.
  /// </summary>
  /// <remarks>
  ///   The storage adapters (tables, queue, object storage) and the authentication services are registered separately.
  /// </remarks>
  /// <param name="services">Collection of services to fill</param>
  /// <param name="configuration">Configuration of the application</param>
  /// <returns>
  ///   The collection of services
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddArmoniKControlPlane(this IServiceCollection services,
                                                          IConfiguration          configuration)
  {
    services.AddUuidGeneratorDefault()
            .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
            .AddSingletonWithHealthCheck<ExceptionInterceptor>(nameof(ExceptionInterceptor))
            .AddInitializedOption<Submitter>(configuration,
                                             Submitter.SettingSection)
            .AddInitializedOption<InitServices>(configuration,
                                                InitServices.SettingSection)
            .AddSingleton<InitDatabase>()
            .AddExceptionManager(sp => ExceptionManager.Options.FromSubmitterOptions(sp.GetRequiredService<Submitter>()))
            .AddGrpcReflection()
            .AddSingleton<MeterHolder>()
            .AddSingleton<AgentIdentifier>()
            .AddScoped(typeof(FunctionExecutionMetrics<>))
            .ValidateGrpcRequests()
            // Asks the agents to stop the tasks that are cancelled; can be overridden by registering another implementation
            .AddHttpClient()
            .AddTransient<ICancelledTaskNotifier, HttpCancelledTaskNotifier>();

    services.AddHealthChecks();
    services.AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>());

    return services;
  }

  /// <summary>
  ///   Map the gRPC services of the control plane
  /// </summary>
  /// <param name="endpoints">Endpoints of the application</param>
  /// <param name="configure">Optional configuration applied to each mapped service, for instance to enable gRPC-Web</param>
  /// <returns>
  ///   The endpoints of the application
  /// </returns>
  [PublicAPI]
  public static IEndpointRouteBuilder MapArmoniKControlPlane(this IEndpointRouteBuilder          endpoints,
                                                             Action<IEndpointConventionBuilder>? configure = null)
  {
    configure ??= _ =>
                  {
                  };

    configure(endpoints.MapGrpcService<GrpcSubmitterService>());
    configure(endpoints.MapGrpcService<GrpcTasksService>());
    configure(endpoints.MapGrpcService<GrpcSessionsService>());
    configure(endpoints.MapGrpcService<GrpcResultsService>());
    configure(endpoints.MapGrpcService<GrpcApplicationsService>());
    configure(endpoints.MapGrpcService<GrpcAuthService>());
    configure(endpoints.MapGrpcService<GrpcEventsService>());
    configure(endpoints.MapGrpcService<GrpcPartitionsService>());
    configure(endpoints.MapGrpcService<GrpcVersionsService>());
    configure(endpoints.MapGrpcService<GrpcHealthChecksService>());

    return endpoints;
  }

  /// <summary>
  ///   Register the services of the compute plane: the pollster, its processing pipeline, and the connections to the
  ///   worker and from the worker (agent).
  /// </summary>
  /// <remarks>
  ///   The storage adapters (tables, queue, object storage) and <see cref="LoggerInit" /> are registered separately.
  ///   <see cref="AgentIdentifier" /> and <see cref="ITaskProcessingChecker" /> can be overridden by registering another
  ///   implementation afterwards.
  /// </remarks>
  /// <param name="services">Collection of services to fill</param>
  /// <param name="configuration">Configuration of the application</param>
  /// <param name="pollsterOptions">Options of the pollster</param>
  /// <returns>
  ///   The collection of services
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddArmoniKComputePlane(this IServiceCollection services,
                                                          IConfiguration          configuration,
                                                          Options.Pollster        pollsterOptions)
    => services.AddArmoniKWorkerConnection(configuration)
               .AddHostedService<PollsterWorker>()
               .AddHostedService<RunningTaskProcessor>()
               .AddHostedService<PostProcessor>()
               .AddSingleton<RunningTaskQueue>()
               .AddSingleton<PostProcessingTaskQueue>()
               .AddSingletonWithHealthCheck<Pollster.Pollster>(nameof(Pollster.Pollster))
               .AddUuidGeneratorDefault()
               .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
               .AddInitializedOption<Submitter>(configuration,
                                                Submitter.SettingSection)
               .AddInitializedOption<InitServices>(configuration,
                                                   InitServices.SettingSection)
               .AddSingleton<InitDatabase>()
               .AddSingleton(pollsterOptions)
               .AddSingleton<HealthCheckRecord>()
               .AddExceptionManager(sp => ExceptionManager.Options.FromPollsterOptions(sp.GetRequiredService<Options.Pollster>()))
               .AddSingleton<IHealthCheckPublisher, HealthCheckRecord.Publisher>()
               .AddSingleton<IAgentHandler, AgentHandler>()
               .AddSingleton<DataPrefetcher>()
               .AddSingleton<MeterHolder>()
               .AddSingleton<AgentIdentifier>()
               .AddScoped(typeof(FunctionExecutionMetrics<>))
               .AddSingleton<ITaskProcessingChecker, TaskProcessingCheckerClient>()
               .AddHttpClient();
}
