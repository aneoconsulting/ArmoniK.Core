// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

using static ArmoniK.Api.gRPC.V1.Tasks.TaskRaw.Types;

namespace ArmoniK.Core.Common.Storage;

public static class TaskLifeCycleHelper
{
  public static async Task CompleteTaskAsync(ITaskTable            taskTable,
                                             IResultTable          resultTable,
                                             IObjectStorageFactory objectStorageFactory,
                                             IPushQueueStorage     pushQueueStorage,
                                             TaskData              taskData,
                                             bool                  resubmit,
                                             Output                output,
                                             ILogger               logger,
                                             CancellationToken     cancellationToken = default)
  {
    Storage.Output cOutput = output;

    if (cOutput.Success)
    {
      await taskTable.SetTaskSuccessAsync(taskData.TaskId,
                                          cancellationToken)
                     .ConfigureAwait(false);

      logger.LogInformation("Remove input payload of {task}",
                            taskData.TaskId);

      //Discard value is used to remove warnings CS4014 !!
      _ = Task.Factory.StartNew(async () => await objectStorageFactory.CreateObjectStorage(taskData.SessionId)
                                                                      .TryDeleteAsync(taskData.TaskId,
                                                                                      CancellationToken.None)
                                                                      .ConfigureAwait(false),
                                cancellationToken);
    }
    else
    {
      // not done means that another pod put this task in error so we do not need to do it a second time
      // so nothing to do
      if (!await taskTable.SetTaskErrorAsync(taskData.TaskId,
                                             cOutput.Error,
                                             cancellationToken)
                          .ConfigureAwait(false))
      {
        return;
      }

      // TODO FIXME: nothing will resubmit the task if there is a crash there
      if (resubmit && taskData.RetryOfIds.Count < taskData.Options.MaxRetries)
      {
        logger.LogWarning("Resubmit {task}",
                          taskData.TaskId);

        var newTaskId = await taskTable.RetryTask(taskData,
                                                  cancellationToken)
                                       .ConfigureAwait(false);

        await FinalizeTaskCreation(taskTable,
                                   resultTable,
                                   pushQueueStorage,
                                   new List<Storage.TaskRequest>
                                   {
                                     new(newTaskId,
                                         taskData.ExpectedOutputIds,
                                         taskData.DataDependencies),
                                   },
                                   taskData.Options.Priority,
                                   taskData.Options.PartitionId,
                                   taskData.SessionId,
                                   taskData.TaskId,
                                   logger,
                                   cancellationToken)
          .ConfigureAwait(false);
      }
      else
      {
        await resultTable.AbortTaskResults(taskData.SessionId,
                                           taskData.TaskId,
                                           cancellationToken)
                         .ConfigureAwait(false);
      }
    }
  }

  public static async Task FinalizeTaskCreation(ITaskTable               taskTable,
                                                IResultTable             resultTable,
                                                IPushQueueStorage        pushQueueStorage,
                                                IEnumerable<TaskRequest> requests,
                                                int                      priority,
                                                string                   partitionId,
                                                string                   sessionId,
                                                string                   parentTaskId,
                                                ILogger                  logger,
                                                CancellationToken        cancellationToken)
  {
    using var _       = logger.LogFunction($"{sessionId}.{parentTaskId}");
    var       taskIds = requests.Select(request => request.Id);

    var parentExpectedOutputKeys = new List<string>();

    if (!parentTaskId.Equals(sessionId))
    {
      parentExpectedOutputKeys.AddRange(await taskTable.GetTaskExpectedOutputKeys(parentTaskId,
                                                                                  cancellationToken)
                                                       .ConfigureAwait(false));
    }

    var taskDataModels = requests.Select(request =>
                                         {
                                           var intersect = parentExpectedOutputKeys.Intersect(request.ExpectedOutputKeys)
                                                                                   .ToList();

                                           var resultModel = request.ExpectedOutputKeys.Except(intersect)
                                                                    .Select(key => new Result(sessionId,
                                                                                              key,
                                                                                              request.Id,
                                                                                              ResultStatus.Created,
                                                                                              DateTime.UtcNow,
                                                                                              Array.Empty<byte>()));

                                           return (Result: resultModel, Req: new IResultTable.ChangeResultOwnershipRequest(intersect,
                                                                                                                           request.Id));
                                         });

    await resultTable.ChangeResultOwnership(sessionId,
                                            parentTaskId,
                                            taskDataModels.Select(tuple => tuple.Req),
                                            cancellationToken)
                     .ConfigureAwait(false);

    await resultTable.Create(taskDataModels.SelectMany(task => task.Result),
                             cancellationToken)
                     .ConfigureAwait(false);

    await pushQueueStorage.PushMessagesAsync(taskIds,
                                             partitionId,
                                             priority,
                                             cancellationToken)
                          .ConfigureAwait(false);

    await taskTable.FinalizeTaskCreation(taskIds,
                                         cancellationToken)
                   .ConfigureAwait(false);
  }


  public static async Task<bool> CheckTaskDependencies(TaskData          taskData,
                                                       IResultTable      resultTable,
                                                       ILogger           logger,
                                                       CancellationToken cancellationToken)
  {
    if (!taskData.DataDependencies.Any())
    {
      return true;
    }

    var dependencies = await resultTable.AreResultsAvailableAsync(taskData.SessionId,
                                                                  taskData.DataDependencies,
                                                                  cancellationToken)
                                        .ConfigureAwait(false);

    if (dependencies.Any())
    {
      return dependencies.SingleOrDefault(i => i.Status == ResultStatus.Completed,
                                          new ResultStatusCount(ResultStatus.Completed,
                                                                0))
                         .Count != taskData.DataDependencies.Count;
    }

    logger.LogDebug("Dependencies are not ready yet.");
    return false;
  }
}
