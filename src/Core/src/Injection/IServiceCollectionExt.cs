// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Injection
{
  public static class IServiceCollectionExt
  {
    public static IServiceCollection AddArmoniKCore(this IServiceCollection services,
                                                    IConfiguration          configuration)
      => services.Configure<ComputePlan>(configuration.GetSection(ComputePlan.SettingSection))
                 .Configure<GrpcChannel>(configuration.GetSection(GrpcChannel.SettingSection))
                 .Configure<Components>(configuration.GetSection(Components.SettingSection))
                 .AddSingleton<GrpcChannelProvider>()
                 .AddSingleton<ClientServiceProvider>()
                 .AddSingleton(typeof(KeyValueStorage<,>));
  }
}
