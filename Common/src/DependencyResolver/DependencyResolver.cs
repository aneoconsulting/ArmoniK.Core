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

using Microsoft.Extensions.Hosting;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.DependencyResolver;

public class DependencyResolver : BackgroundService, IInitializable
{
  private readonly IPullQueueStorage           pullQueueStorage_;
  private readonly IPushQueueStorage           pushQueueStorage_;
  private readonly ITaskTable                  taskTable_;
  private readonly IResultTable                resultTable_;
  private readonly ILogger<DependencyResolver> logger_;

  public DependencyResolver(IPullQueueStorage           pullQueueStorage,
                            IPushQueueStorage           pushQueueStorage,
                            ITaskTable                  taskTable,
                            IResultTable                resultTable,
                            ILogger<DependencyResolver> logger)
  {
    pullQueueStorage_ = pullQueueStorage;
    pushQueueStorage_ = pushQueueStorage;
    taskTable_        = taskTable;
    resultTable_      = resultTable;
    logger_           = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    while (!stoppingToken.IsCancellationRequested)
    {
      try
      {
        var messages = pullQueueStorage_.PullMessagesAsync(1,
                                                           stoppingToken);

        await foreach (var message in messages.WithCancellation(stoppingToken)
                                              .ConfigureAwait(false))
        {
          using var scopedLogger = logger_.BeginNamedScope("Prefetch messageHandler",
                                                           ("messageHandler", message.MessageId),
                                                           ("taskId", message.TaskId));

          var taskData = await taskTable_.ReadTaskAsync(message.TaskId,
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

          var areDependenciesOk = await TaskLifeCycleHelper.CheckTaskDependencies(taskData,
                                                                                  resultTable_,
                                                                                  logger_,
                                                                                  stoppingToken)
                                                           .ConfigureAwait(false);
          if (areDependenciesOk)
          {
            await pushQueueStorage_.PushMessagesAsync(new[]
                                                      {
                                                        taskData.TaskId,
                                                      },
                                                      taskData.Options.PartitionId,
                                                      taskData.Options.Priority,
                                                      stoppingToken)
                                   .ConfigureAwait(false);
          }

          message.Status = QueueMessageStatus.Processed;
          await message.DisposeAsync()
                       .ConfigureAwait(false);
        }
      }
      catch (Exception ex)
      {
        throw;
      }
    }
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;
}
