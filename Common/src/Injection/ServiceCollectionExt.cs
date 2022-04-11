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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Injection.Options;

using Calzolari.Grpc.AspNetCore.Validation;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Injection;

public static class ConfigurationExt
{
  public static T GetRequiredValue<T>(this IConfiguration configuration,
                                      string              key)
    => configuration.GetRequiredSection(key)
                    .Get<T>();
}

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddOption<T>(this IServiceCollection services,
                                                IConfiguration          configuration,
                                                string                  key)
    where T : class
    => services.AddSingleton(configuration.GetRequiredValue<T>(key));


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

  [PublicAPI]
  public static IServiceCollection AddArmoniKWorkerConnection(this IServiceCollection services,
                                                              IConfiguration          configuration)
  {
    var computePlanComponent = configuration.GetSection(ComputePlan.SettingSection);
    if (!computePlanComponent.Exists())
    {
      return services;
    }

    var computePlanOptions = computePlanComponent.Get<ComputePlan>();

    if (computePlanOptions.GrpcChannel is not null)
    {
      services.AddSingleton(computePlanOptions)
              .AddSingleton(computePlanOptions.GrpcChannel)
              .AddOption<Components>(configuration,
                                     Components.SettingSection)
              .AddSingletonWithHealthCheck<GrpcChannelProvider>(nameof(GrpcChannelProvider))
              .AddSingletonWithHealthCheck<WorkerClientProvider>(nameof(WorkerClientProvider));
    }

    return services;
  }

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

  [PublicAPI]
  public static IServiceCollection AddSingletonWithHealthCheck<TService, TImplementation>(this IServiceCollection services,
                                                                                          string                  checkName)
    where TImplementation : class, IHealthCheckProvider, TService
    where TService : class
  {
    services.AddSingleton<TService, TImplementation>();

    services.AddHealthChecks()
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Startup)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Startup),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Startup),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Liveness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Liveness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Liveness),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Readiness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Readiness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Readiness),
                                             }));
    return services;
  }

  [PublicAPI]
  public static IServiceCollection AddTransientWithHealthCheck<T>(this IServiceCollection services,
                                                                  string                  checkName)
    where T : class, IHealthCheckProvider
  {
    services.AddTransient<T>();

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

  [PublicAPI]
  public static IServiceCollection AddTransientWithHealthCheck<TService, TImplementation>(this IServiceCollection services,
                                                                                          string                  checkName)
    where TImplementation : class, IHealthCheckProvider, TService
    where TService : class
  {
    services.AddTransient<TService, TImplementation>();

    services.AddHealthChecks()
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Startup)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Startup),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Startup),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Liveness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Liveness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Liveness),
                                             }))
            .Add(new HealthCheckRegistration($"{checkName}.{nameof(HealthCheckTag.Readiness)}",
                                             provider => new HealthCheck(provider.GetRequiredService<TImplementation>(),
                                                                         HealthCheckTag.Readiness),
                                             HealthStatus.Unhealthy,
                                             new[]
                                             {
                                               nameof(HealthCheckTag.Readiness),
                                             }));
    return services;
  }

  public static IServiceCollection AddTransientWithHealthCheck<TService>(this IServiceCollection          services,
                                                                         Func<IServiceProvider, TService> implementationFactory,
                                                                         string                           checkName)
    where TService : class, IHealthCheckProvider
  {
    services.AddTransient(implementationFactory);

    services.AddHealthChecks()
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
    return services;
  }

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
               .AddValidator<TaskOptionsValidator>()
               .AddValidator<TaskFilterValidator>()
               .AddGrpcReflection()
               .AddGrpcValidation();
}
