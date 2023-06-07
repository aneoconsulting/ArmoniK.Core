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

using System.Collections.Generic;

using Armonik.Api.Grpc.V1.Partitions;
using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.ListPartitionsRequestExt;

[TestFixture(TestOf = typeof(ToPartitionDataFieldTest))]
public class ToPartitionDataFieldTest
{
  private static readonly PartitionData PartitionData = new("PartitionId",
                                                            new List<string>
                                                            {
                                                              "ParentPartitionId",
                                                            },
                                                            1,
                                                            10,
                                                            15,
                                                            2,
                                                            new PodConfiguration(new Dictionary<string, string>()));


  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    TestCaseData Case(PartitionRawField field,
                      object?           expected)
      => new TestCaseData(field,
                          expected).SetArgDisplayNames(field.ToString());

    yield return Case(PartitionRawField.ParentPartitionIds,
                      PartitionData.ParentPartitionIds);
    yield return Case(PartitionRawField.Id,
                      PartitionData.PartitionId);
    yield return Case(PartitionRawField.PodMax,
                      PartitionData.PodMax);
    yield return Case(PartitionRawField.PodReserved,
                      PartitionData.PodReserved);
    yield return Case(PartitionRawField.Priority,
                      PartitionData.Priority);
    yield return Case(PartitionRawField.PreemptionPercentage,
                      PartitionData.PreemptionPercentage);
  }

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(PartitionRawField field,
                                              object?           expected)
  {
    var func = new ListPartitionsRequest
               {
                 Filter = new ListPartitionsRequest.Types.Filter
                          {
                            Id = "PartitionId",
                          },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field = new PartitionField
                                  {
                                    PartitionRawField = field,
                                  },
                        },
               }.Sort.ToPartitionField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(PartitionData));
  }
}
