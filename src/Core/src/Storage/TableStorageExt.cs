// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Storage
{
  public static class TableStorageExt
  {

    public static async Task<bool> IsTaskCompleted(this ITableStorage tableStorage, TaskData taskData, CancellationToken cancellationToken = default)
    {
      var status = taskData.Status;
      if (status != TaskStatus.Completed)
        return false;

      if (taskData.Options.Dependencies.Count == 0)
        return true;

      var cts          = new CancellationTokenSource();
      var aggregateCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);

      var futureDependenciesData = taskData.Options.Dependencies.Select(async id =>
                                                                        {
                                                                          var depTaskData = await tableStorage.ReadTaskAsync(id, aggregateCts.Token);
                                                                          return await tableStorage.IsTaskCompleted(depTaskData, aggregateCts.Token);
                                                                        }).ToList(); // ToList ensures that all operations have started before processing results

      while (futureDependenciesData.Count > 0)
      {
        var finished = await Task.WhenAny(futureDependenciesData);
        futureDependenciesData.Remove(finished);

        if (finished.Result)
          continue;

        cts.Cancel();
        try
        {
          await Task.WhenAll(futureDependenciesData); // avoid dandling running Tasks
        }
        catch (OperationCanceledException) { }

        return false;
      }

      return true;
    }
  }
}
