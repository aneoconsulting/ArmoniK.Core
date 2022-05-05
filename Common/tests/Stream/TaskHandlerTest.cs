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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Stream;

[TestFixture]
public class TaskHandlerTest
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  [Test]
  public async Task EmptyPayloadAndOneDependency()
  {
    var loggerFactory = new LoggerFactory();


    var computeRequests = new List<ProcessRequest>();

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      InitRequest = new ProcessRequest.Types.ComputeRequest.Types.InitRequest
                                                    {
                                                      Configuration = new Configuration(),
                                                      Payload       = new DataChunk(),
                                                      TaskId        = "MyTaskId",
                                                      SessionId     = "MySessionId",
                                                      ExpectedOutputKeys =
                                                      {
                                                        "MyOutput",
                                                      },
                                                    },
                                    },
                        });

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      Payload = new DataChunk
                                                {
                                                  DataComplete = true,
                                                },
                                    },
                        });

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
                                                 {
                                                   Key = "KeyData",
                                                 },
                                    },
                        });

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      Data = new DataChunk
                                             {
                                               Data = ByteString.Empty,
                                             },
                                    },
                        });

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      Data = new DataChunk
                                             {
                                               DataComplete = true,
                                             },
                                    },
                        });

    computeRequests.Add(new ProcessRequest
                        {
                          Compute = new ProcessRequest.Types.ComputeRequest
                                    {
                                      InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
                                                 {
                                                   LastData = true,
                                                 },
                                    },
                        });

    IAsyncStreamReader<ProcessRequest> requestStream  = new TestHelperAsyncStreamReader<ProcessRequest>(computeRequests);
    var                                responseStream = new TestHelperServerStreamWriter<ProcessReply>();
    var taskHandler = await TaskHandler.Create(requestStream,
                                               responseStream,
                                               new Configuration
                                               {
                                                 DataChunkMaxSize = 50 * 1024,
                                               },
                                               loggerFactory.CreateLogger<TaskHandler>(),
                                               CancellationToken.None)
                                       .ConfigureAwait(false);

    await taskHandler.SendResult(taskHandler.ExpectedResults.Single(),
                                 Convert.FromBase64String("AAAA1111"))
                     .ConfigureAwait(false);

    foreach (var response in responseStream.Messages)
    {
      Console.WriteLine(response);
    }

    Assert.AreEqual(responseStream.Messages[0]
                                  .TypeCase,
                    ProcessReply.TypeOneofCase.Result);
    Assert.AreEqual(responseStream.Messages[0]
                                  .Result.TypeCase,
                    ProcessReply.Types.Result.TypeOneofCase.Init);
    Assert.AreEqual(responseStream.Messages[0]
                                  .Result.Init.TypeCase,
                    InitKeyedDataStream.TypeOneofCase.Key);
    Assert.AreEqual("MyOutput",
                    responseStream.Messages[0]
                                  .Result.Init.Key);
    Assert.AreEqual(responseStream.Messages[1]
                                  .TypeCase,
                    ProcessReply.TypeOneofCase.Result);
    Assert.AreEqual(responseStream.Messages[1]
                                  .Result.TypeCase,
                    ProcessReply.Types.Result.TypeOneofCase.Data);
    Assert.AreEqual(responseStream.Messages[1]
                                  .Result.Data.Data,
                    ByteString.CopyFrom(Convert.FromBase64String("AAAA1111")));
    Assert.AreEqual(responseStream.Messages[2]
                                  .TypeCase,
                    ProcessReply.TypeOneofCase.Result);
    Assert.AreEqual(responseStream.Messages[2]
                                  .Result.TypeCase,
                    ProcessReply.Types.Result.TypeOneofCase.Data);
    Assert.IsTrue(responseStream.Messages[2]
                                .Result.Data.DataComplete);
    Assert.AreEqual(responseStream.Messages[3]
                                  .TypeCase,
                    ProcessReply.TypeOneofCase.Result);
    Assert.AreEqual(responseStream.Messages[3]
                                  .Result.TypeCase,
                    ProcessReply.Types.Result.TypeOneofCase.Init);
    Assert.IsTrue(responseStream.Messages[3]
                                .Result.Init.LastResult);
  }
}
