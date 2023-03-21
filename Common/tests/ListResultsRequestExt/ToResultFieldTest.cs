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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

[TestFixture(TestOf = typeof(ToResultFieldTest))]
public class ToResultFieldTest
{
  private readonly Result result_ = new("SessionId",
                                        "Name",
                                        "OwnerTaskId",
                                        ResultStatus.Created,
                                        new List<string>(),
                                        DateTime.UtcNow,
                                        Array.Empty<byte>());

  [Test]
  public void InvokeShouldReturnCreationDate()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.CreationDate,
                    func.Invoke(result_));
  }

  [Test]
  public void InvokeShouldReturnSessionId()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.SessionId,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.SessionId,
                    func.Invoke(result_));
  }

  [Test]
  public void InvokeShouldReturnStatus()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.Status,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.Status,
                    func.Invoke(result_));
  }

  [Test]
  public void InvokeShouldReturnName()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.Name,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.Name,
                    func.Invoke(result_));
  }

  [Test]
  public void InvokeShouldReturnOwnerTaskId()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.OwnerTaskId,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.OwnerTaskId,
                    func.Invoke(result_));
  }
}
