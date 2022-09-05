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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListSessionsRequestExt;

[TestFixture(TestOf = typeof(ToSessionDataFieldTest))]
public class ToSessionDataFieldTest
{
  private static readonly TaskOptions Options = new(new Dictionary<string, string>(),
                                                    TimeSpan.MaxValue,
                                                    5,
                                                    1,
                                                    "part1",
                                                    "applicationName",
                                                    "applicationVersion",
                                                    "applicationNamespace",
                                                    "applicationService",
                                                    "engineType");

  private readonly SessionData sessionData_ = new("SessionId",
                                                  SessionStatus.Running,
                                                  DateTime.UtcNow,
                                                  DateTime.UtcNow,
                                                  new List<string>(),
                                                  Options);

  [Test]
  public void InvokeShouldReturnCreationDate()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
               }.Sort.ToSessionDataField()
                .Compile();

    Assert.AreEqual(sessionData_.CreationDate,
                    func.Invoke(sessionData_));
  }

  [Test]
  public void InvokeShouldReturnSessionId()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.SessionId,
                        },
               }.Sort.ToSessionDataField()
                .Compile();

    Assert.AreEqual(sessionData_.SessionId,
                    func.Invoke(sessionData_));
  }

  [Test]
  public void InvokeShouldReturnStatus()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.Status,
                        },
               }.Sort.ToSessionDataField()
                .Compile();

    Assert.AreEqual(sessionData_.Status,
                    func.Invoke(sessionData_));
  }

  [Test]
  public void InvokeShouldReturnCancelledAt()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CancelledAt,
                        },
               }.Sort.ToSessionDataField()
                .Compile();

    Assert.AreEqual(sessionData_.CancellationDate,
                    func.Invoke(sessionData_));
  }
}
