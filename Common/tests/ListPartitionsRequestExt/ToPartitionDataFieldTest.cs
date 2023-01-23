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

using System.Collections.Generic;

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.ListPartitionsRequestExt;

[TestFixture(TestOf = typeof(ToPartitionDataFieldTest))]
public class ToPartitionDataFieldTest
{
  private readonly PartitionData partitionData_ = new("PartitionId",
                                                      new List<string>
                                                      {
                                                        "ParentPartitionId",
                                                      },
                                                      1,
                                                      10,
                                                      15,
                                                      2,
                                                      new PodConfiguration(new Dictionary<string, string>()));

  [Test]
  public void InvokeShouldReturnPriority()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Priority,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.Priority,
                    func.Invoke(partitionData_));
  }

  [Test]
  public void InvokeShouldReturnPodMax()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.PodMax,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.PodMax,
                    func.Invoke(partitionData_));
  }

  [Test]
  public void InvokeShouldReturnParentPartitionIds()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.ParentPartitionIds,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.ParentPartitionIds,
                    func.Invoke(partitionData_));
  }

  [Test]
  public void InvokeShouldReturnParentPodReserved()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.PodReserved,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.PodReserved,
                    func.Invoke(partitionData_));
  }

  [Test]
  public void InvokeShouldReturnId()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.PartitionId,
                    func.Invoke(partitionData_));
  }

  [Test]
  public void InvokeShouldReturnPreemptionPercentage()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.PreemptionPercentage,
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(partitionData_.PreemptionPercentage,
                    func.Invoke(partitionData_));
  }
}
