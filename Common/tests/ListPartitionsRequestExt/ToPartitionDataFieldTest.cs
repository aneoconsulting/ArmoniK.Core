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

using Armonik.Api.gRPC.V1;
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
    TestCaseData Case(PartitionRawEnumField field,
                      object?               expected)
      => new TestCaseData(new PartitionRawField
                          {
                            Field = field,
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return Case(PartitionRawEnumField.ParentPartitionIds,
                      PartitionData.ParentPartitionIds);
    yield return Case(PartitionRawEnumField.Id,
                      PartitionData.PartitionId);
    yield return Case(PartitionRawEnumField.PodMax,
                      PartitionData.PodMax);
    yield return Case(PartitionRawEnumField.PodReserved,
                      PartitionData.PodReserved);
    yield return Case(PartitionRawEnumField.Priority,
                      PartitionData.Priority);
    yield return Case(PartitionRawEnumField.PreemptionPercentage,
                      PartitionData.PreemptionPercentage);
  }

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(PartitionRawField field,
                                              object?           expected)
  {
    var func = new ListPartitionsRequest
               {
                 Filters = new Filters
                           {
                             Filters_ = new FiltersOr
                                        {
                                          Filters =
                                          {
                                            new FiltersAnd
                                            {
                                              Filters =
                                              {
                                                new FilterField
                                                {
                                                  String = new FilterString
                                                           {
                                                             Field = new PartitionField
                                                                     {
                                                                       PartitionRawField = new PartitionRawField
                                                                                           {
                                                                                             Field = PartitionRawEnumField.Id,
                                                                                           },
                                                                     },
                                                             Operator = FilterStringOperator.Equal,
                                                             Value    = "PartitionId",
                                                           },
                                                },
                                              },
                                            },
                                          },
                                        },
                           },
                 Sort = new ListPartitionsRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field = new PartitionField
                                  {
                                    PartitionRawField = field,
                                  },
                        },
               }.Sort.ToField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(PartitionData));
  }
}
