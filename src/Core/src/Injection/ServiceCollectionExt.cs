// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
    {
      return services.Configure<ComputePlan>(configuration.GetSection(ComputePlan.SettingSection))
                     .Configure<GrpcChannel>(configuration.GetSection(GrpcChannel.SettingSection))
                     .Configure<Components>(configuration.GetSection(Components.SettingSection))
                     .AddSingleton<GrpcChannelProvider>()
                     .AddSingleton<ClientServiceProvider>()
                     .AddTransient<IObjectStorage, DistributedCacheObjectStorage>()
                     .AddSingleton(typeof(KeyValueStorage<,>));
    }

    public static IServiceCollection ValidateGrpcRequests(this IServiceCollection services)
    {
      return services.AddGrpc(options =>
                     {
                       options.EnableMessageValidation();
                       options.MaxReceiveMessageSize = null;
                     })
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
}
