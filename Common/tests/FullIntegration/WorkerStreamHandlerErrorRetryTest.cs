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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.FullIntegration;

class WorkerStreamHandlerErrorRetryTest : WorkerStreamHandlerBase
{
  private readonly Exception exception_;

  public WorkerStreamHandlerErrorRetryTest(Exception exception)
  {
    exception_ = exception;
  }

  public override void StartTaskProcessing(TaskData          taskData,
                                           CancellationToken cancellationToken)
  {
    if (!taskData.TaskId.Contains("###"))
    {
      throw exception_;
    }

    var task = new Task(async () =>
                        {
                          var request = await ChannelAsyncPipe.Reverse.Reader.FirstAsync(cancellationToken: cancellationToken)
                                                   .ConfigureAwait(false);


                          await ChannelAsyncPipe.Reverse.WriteAsync(new ProcessReply
                                                         {
                                                           Output = new Output
                                                                    {
                                                                      Ok     = new Empty(),
                                                                      Status = TaskStatus.Completed,
                                                                    },
                                                         })
                                     .ConfigureAwait(false);


                        });
    TaskList.Add(task);
    task.Start();
  }
}
