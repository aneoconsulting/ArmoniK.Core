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

[TestFixture(TestOf = typeof(ToPartitionDataFilterTest))]
public class ToPartitionDataFilterTest
{
  private readonly PartitionData partitionData_ = new("PartitionId",
                                                      new List<string>
                                                      {
                                                        "ParentPartitionId1",
                                                        "ParentPartitionId2",
                                                      },
                                                      1,
                                                      10,
                                                      15,
                                                      2,
                                                      new PodConfiguration(new Dictionary<string, string>()));

  [Test]
  public void FilterIdShouldSucceed()
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
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(partitionData_));
  }

  [Test]
  public void FilterWrongIdShouldFail()
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
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(partitionData_ with
                               {
                                 PartitionId = "test",
                               }));
  }

  [Test]
  public void FilterPodReservedShouldSucceed()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PodReserved = 1,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(partitionData_));
  }

  [Test]
  public void FilterWrongPodReservedShouldFail()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PodReserved = 1,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(partitionData_ with
                               {
                                 PodReserved = 2,
                               }));
  }

  [Test]
  public void FilterPodMaxShouldSucceed()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PodMax = 10,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(partitionData_));
  }

  [Test]
  public void FilterWrongPodMaxShouldFail()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PodMax = 10,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(partitionData_ with
                               {
                                 PodMax = 20,
                               }));
  }

  [Test]
  public void FilterPreemptionPercentageShouldSucceed()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PreemptionPercentage = 15,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(partitionData_));
  }

  [Test]
  public void FilterWrongPreemptionPercentageShouldFail()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            PreemptionPercentage = 15,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(partitionData_ with
                               {
                                 PreemptionPercentage = 877,
                               }));
  }

  [Test]
  public void FilterPriorityShouldSucceed()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Priority = 2,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(partitionData_));
  }

  [Test]
  public void FilterWrongPriorityShouldFail()
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Priority = 2,
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = ListPartitionsRequest.Types.OrderDirection.Asc,
                          Field     = ListPartitionsRequest.Types.OrderByField.Id,
                        },
               }.Filter.ToPartitionFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(partitionData_ with
                               {
                                 Priority = 877,
                               }));
  }
}
