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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.Storage;

using Output = ArmoniK.Api.gRPC.V1.Output;

namespace ArmoniK.Core.Common.Tests.FullIntegration;

public class WorkerStreamHandlerFullTest : WorkerStreamHandlerBase
{
  public override void StartTaskProcessing(TaskData          taskData,
                                           CancellationToken cancellationToken)
  {
    Console.WriteLine(taskData);

    var task = new Task(async () =>
                        {
                          //var requests = pipe_.Reverse.Reader.GetAsyncEnumerator(cancellationToken);
                          //while (await requests.MoveNextAsync().ConfigureAwait(false))
                          //{
                          //  Console.WriteLine(requests.Current);
                          //}

                          Console.WriteLine(await ChannelAsyncPipe.Reverse.Read(cancellationToken)
                                                                  .ConfigureAwait(false));

                          await ChannelAsyncPipe.Reverse.WriteAsync(new ProcessReply
                                                                    {
                                                                      Output = new Output
                                                                               {
                                                                                 Ok = new Empty(),
                                                                               },
                                                                    })
                                                .ConfigureAwait(false);

                          await ChannelAsyncPipe.Reverse.CompleteAsync()
                                                .ConfigureAwait(false);
                        });
    TaskList.Add(task);
    task.Start();
  }
}
