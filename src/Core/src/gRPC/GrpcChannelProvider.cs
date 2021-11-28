// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading.Tasks;

using ArmoniK.Core.Injection;
using ArmoniK.Core.Injection.Options;

using Microsoft.Extensions.Options;

using GrpcChannel = Grpc.Net.Client.GrpcChannel;

namespace ArmoniK.Core.gRPC
{


  public class GrpcChannelProvider : ProviderBase<GrpcChannel>
  {
    public GrpcChannelProvider(IOptions<Injection.Options.GrpcChannel> options)
      : base(options.Value.SocketType == GrpcSocketType.Web
               ? () => Task.FromResult(BuildWebGrpcChannel(options.Value.Address))
               : () => Task.FromResult(BuildUnixSocketGrpcChannel(options.Value.Address)))
    {
    }

    private static GrpcChannel BuildWebGrpcChannel(string address) => GrpcChannel.ForAddress(address);
    private static GrpcChannel BuildUnixSocketGrpcChannel(string address) => throw new NotImplementedException();
  }
}
