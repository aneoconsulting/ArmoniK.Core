// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public static class LeaseProviderExt
  {
    public static async Task<LeaseHandler> GetLeaseHandlerAsync(this ILeaseProvider leaseProvider,
                                                                TaskId              taskId,
                                                                ILogger             logger,
                                                                CancellationToken   cancellationToken = default)
    {
      var output = new LeaseHandler(leaseProvider, taskId, logger, cancellationToken);
      await output.Start();
      return output;
    }
  }
}