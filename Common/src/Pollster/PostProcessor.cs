// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
// 
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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;

namespace ArmoniK.Core.Common.Pollster;

public class PostProcessor : BackgroundService
{
  private readonly PostProcessingTaskQueue postProcessingTaskQueue_;
  public           string                  CurrentTask = string.Empty;

  public PostProcessor(PostProcessingTaskQueue postProcessingTaskQueue)
    => postProcessingTaskQueue_ = postProcessingTaskQueue;

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    while (!stoppingToken.IsCancellationRequested)
    {
      var taskHandler = await postProcessingTaskQueue_.ReadAsync(stoppingToken)
                                                      .ConfigureAwait(false);
      try
      {
        CurrentTask = taskHandler.GetAcquiredTask();
        await taskHandler.PostProcessing()
                         .ConfigureAwait(false);
      }
      catch (Exception e)
      {
        postProcessingTaskQueue_.AddException(ExceptionDispatchInfo.Capture(e)
                                                                   .SourceException);
      }
      finally
      {
        await taskHandler.DisposeAsync()
                         .ConfigureAwait(false);
        CurrentTask = string.Empty;
      }
    }
  }
}
