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
                          Field = ListResultsRequest.Types.SortField.CreatedAt,
                          Order = ListResultsRequest.Types.SortOrder.Asc,
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
                          Field = ListResultsRequest.Types.SortField.SessionId,
                          Order = ListResultsRequest.Types.SortOrder.Asc,
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
                          Field = ListResultsRequest.Types.SortField.Status,
                          Order = ListResultsRequest.Types.SortOrder.Asc,
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
                          Field = ListResultsRequest.Types.SortField.Name,
                          Order = ListResultsRequest.Types.SortOrder.Asc,
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
                          Field = ListResultsRequest.Types.SortField.OwnerTaskId,
                          Order = ListResultsRequest.Types.SortOrder.Asc,
                        },
               }.Sort.ToResultField()
                .Compile();

    Assert.AreEqual(result_.OwnerTaskId,
                    func.Invoke(result_));
  }
}
