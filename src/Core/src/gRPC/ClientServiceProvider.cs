// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection;

using System;
using System.Threading.Tasks;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.gRPC
{
  public class ClientServiceProvider : ProviderBase<ComputerService.ComputerServiceClient>
  {
    /// <inheritdoc />
    public ClientServiceProvider(GrpcChannelProvider channelProvider, ILogger<ClientServiceProvider> logger) :
      base(() => BuildClientService(channelProvider, logger))
    {
    }

    private static async Task<ComputerService.ComputerServiceClient> BuildClientService(
      GrpcChannelProvider channelProvider,
      ILogger             logger)
    {
      using var   _ = logger.LogFunction();
      ChannelBase channel;
      try
      {
        channel = await channelProvider.GetAsync();
      }
      catch (Exception e)
      {
        logger.LogError(e, "Could not create grpc channel");
        throw;
      }

      return new ComputerService.ComputerServiceClient(channel);
    }
  }
}