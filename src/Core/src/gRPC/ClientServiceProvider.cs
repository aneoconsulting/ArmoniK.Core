// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection;

namespace ArmoniK.Core.gRPC
{
  public class ClientServiceProvider : ProviderBase<ComputerService.ComputerServiceClient>
  {
    /// <inheritdoc />
    public ClientServiceProvider(GrpcChannelProvider channelProvider) :
      base(async () => new ComputerService.ComputerServiceClient(await channelProvider.GetAsync()))
    {
    }
  }
}