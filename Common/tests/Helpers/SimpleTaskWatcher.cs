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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Helpers;

internal class SimpleTaskWatcher : ITaskWatcher
{
  public const string PayloadId    = "MyPayloadId";
  public const string OutputId     = "MyOutputId";
  public const string SessionId    = "MySessionId";
  public const string TaskId       = "MyTaskId";
  public const string OriginTaskId = "MyOriginTaskId";

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<IAsyncEnumerable<NewTask>> GetNewTasks(Expression<Func<TaskData, bool>> filter,
                                                     CancellationToken                cancellationToken = default)
    => Task.FromResult(new[]
                       {
                         new NewTask(SessionId,
                                     TaskId,
                                     OriginTaskId,
                                     PayloadId,
                                     new List<string>
                                     {
                                       SessionId,
                                     },
                                     new List<string>
                                     {
                                       OutputId,
                                     },
                                     new List<string>(),
                                     new List<string>(),
                                     TaskStatus.Creating),
                       }.ToAsyncEnumerable());

  public Task<IAsyncEnumerable<TaskStatusUpdate>> GetTaskStatusUpdates(Expression<Func<TaskData, bool>> filter,
                                                                       CancellationToken                cancellationToken = default)
    => Task.FromResult(new[]
                       {
                         new TaskStatusUpdate(SessionId,
                                              TaskId,
                                              TaskStatus.Submitted),
                         new TaskStatusUpdate(SessionId,
                                              TaskId,
                                              TaskStatus.Processing),
                         new TaskStatusUpdate(SessionId,
                                              TaskId,
                                              TaskStatus.Completed),
                       }.ToAsyncEnumerable());
}
