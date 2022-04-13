// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Control.Metrics;

public class ArmoniKMeter : Meter, IHostedService
{
  private int i;

  public ArmoniKMeter(ITaskTable            taskTable,
                      ILogger<ArmoniKMeter> logger)
    : base(nameof(ArmoniKMeter))
  {
    using var _ = logger.LogFunction();
    foreach (var status in (TaskStatus[])Enum.GetValues(typeof(TaskStatus)))
    {
      CreateObservableGauge("armonik_tasks_" + status.ToString()
                                                     .ToLower(),
                            () => new Measurement<int>(taskTable.CountAllTasksAsync(status)
                                                                .Result));
    }

    CreateObservableGauge("armonik_tasks_queued",
                          () => new Measurement<int>(taskTable.CountAllTasksAsync(TaskStatus.Dispatched)
                                                              .Result                                       + taskTable.CountAllTasksAsync(TaskStatus.Submitted)
                                                                                                    .Result + taskTable.CountAllTasksAsync(TaskStatus.Processing)
                                                                                                                       .Result));

    CreateObservableCounter("test",
                            () => i++);
    logger.LogDebug("Meter added");
  }

  public Task StartAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task StopAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;
}
