// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Control.Submitter.Tests;

[TestFixture(TestOf = typeof(Services.Submitter))]
public class OpenSession
{
  [Test]
  public void HappyFlow()
  {    
    var request = new CreateSessionRequest()
                  {
                    Root = new()
                           {
                             Id = "Id",
                    DefaultTaskOption = new()
                                        {
                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                          MaxRetries = 2,
                                          Priority = 5,
                                        },
                           },
                  };

    var token = CancellationToken.None;

    var tableStorageMock = new Mock<ITableStorage>();
    tableStorageMock.Setup(storage => storage.CreateSessionAsync(request,
                                                                 token))
                    .ReturnsAsync(new CreateSessionReply
                                  {
                                    Ok = new(),
                                  })
                    .Verifiable();

    var tableStorage = tableStorageMock.Object;

    var lockedQueueStorage = new Mock<IQueueStorage>().Object;

    var objectStorageFactory = new Mock<IObjectStorageFactory>().Object;

    var server = new Services.Submitter(tableStorage,
                                        lockedQueueStorage,
                                        objectStorageFactory,
                                        NullLogger<Services.Submitter>.Instance);



    var contextMock = new Mock<ServerCallContext>();
    contextMock.Setup(context => context.CancellationToken).Returns(token);
    var context = contextMock.Object;

    var reply = server.CreateSession(request, context).Result;

    Assert.AreEqual(CreateSessionReply.ResultOneofCase.Ok, reply.ResultCase);
  }
}