// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.Validators;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;

using Calzolari.Grpc.AspNetCore.Validation;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Injection
{
  public static class ServiceCollectionExt
  {
    public static IServiceCollection AddArmoniKCore(this IServiceCollection services,
                                                    IConfiguration          configuration)
      => services.Configure<ComputePlan>(configuration.GetSection(ComputePlan.SettingSection))
                 .Configure<GrpcChannel>(configuration.GetSection(GrpcChannel.SettingSection))
                 .Configure<Components>(configuration.GetSection(Components.SettingSection))
                 .AddSingleton<GrpcChannelProvider>()
                 .AddSingleton<ClientServiceProvider>()
                 .AddTransient<IObjectStorage, DistributedCacheObjectStorage>()
                 .AddSingleton(typeof(KeyValueStorage<,>));

    public static IServiceCollection ValidateGrpcRequests(this IServiceCollection services)
      => services.AddGrpc(options => options.EnableMessageValidation())
                 .Services
                 .AddValidator<CreateTaskRequestValidator>()
                 .AddValidator<PayloadValidator>()
                 .AddValidator<SessionIdValidator>()
                 .AddValidator<SessionOptionsValidator>()
                 .AddValidator<TaskIdValidator>()
                 .AddValidator<TaskOptionsValidator>()
                 .AddValidator<TaskRequestValidator>()
                 .AddGrpcValidation();
  }
}