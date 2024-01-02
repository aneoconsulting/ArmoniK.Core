// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.SortDirection;
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

  private static readonly ListPartitionsRequest.Types.Sort Sort = new()
                                                                  {
                                                                    Direction = SortDirection.Asc,
                                                                    Field = new PartitionField
                                                                            {
                                                                              PartitionRawField = new PartitionRawField
                                                                                                  {
                                                                                                    Field = PartitionRawEnumField.Id,
                                                                                                  },
                                                                            },
                                                                  };


  private static Func<PartitionData, bool> RequestToFunc(ListPartitionsRequest.Types.Sort sort,
                                                         IEnumerable<FilterField>         filterFields)
    => ListPartitionsHelper.CreateListPartitionsRequest(sort,
                                                        filterFields)
                           .Filters.ToPartitionFilter()
                           .Compile();


  [Test]
  [TestCaseSource(nameof(TestCasesFilter))]
  public void Filter(IEnumerable<FilterField> filterFields,
                     bool                     expected)
  {
    var func = RequestToFunc(Sort,
                             filterFields);

    Assert.AreEqual(expected,
                    func.Invoke(partitionData_));
  }

  public static IEnumerable<TestCaseData> TestCasesFilter()
  {
    TestCaseData CaseTrue(FilterField filterField)
      => new TestCaseData(new[]
                          {
                            filterField,
                          },
                          true).SetArgDisplayNames(filterField.ToString());

    TestCaseData CaseFalse(FilterField filterField)
      => new TestCaseData(new[]
                          {
                            filterField,
                          },
                          false).SetArgDisplayNames(filterField.ToString());

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                FilterStringOperator.Equal,
                                                                                "PartitionId"));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                 FilterStringOperator.Equal,
                                                                                 "PartitionId_false"));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodReserved,
                                                                                FilterNumberOperator.Equal,
                                                                                1));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodReserved,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodMax,
                                                                                FilterNumberOperator.Equal,
                                                                                10));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodMax,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PreemptionPercentage,
                                                                                FilterNumberOperator.Equal,
                                                                                15));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PreemptionPercentage,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.Priority,
                                                                                FilterNumberOperator.Equal,
                                                                                2));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.Priority,
                                                                                 FilterNumberOperator.Equal,
                                                                                 1));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                               FilterArrayOperator.Contains,
                                                                               "ParentPartitionId1"));
    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                               FilterArrayOperator.NotContains,
                                                                               "AnotherParentPartitionId1"));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                                FilterArrayOperator.Contains,
                                                                                "AnotherParentPartitionId1"));
  }
}
