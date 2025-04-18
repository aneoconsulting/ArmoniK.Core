// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

[TestFixture(TestOf = typeof(ToResultFieldTest))]
public class ToResultFieldTest
{
  private static readonly Result Result = new("SessionId",
                                              "ResultId",
                                              "Name",
                                              "CreatedBy",
                                              "CompletedBy",
                                              "OwnerTaskId",
                                              ResultStatus.Created,
                                              new List<string>(),
                                              DateTime.UtcNow,
                                              null,
                                              0,
                                              Array.Empty<byte>(),
                                              false);

  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    yield return Case(ResultRawEnumField.Status,
                      Result.Status);
    yield return Case(ResultRawEnumField.CreatedAt,
                      Result.CreationDate);
    yield return Case(ResultRawEnumField.CompletedAt,
                      Result.CompletionDate);
    yield return Case(ResultRawEnumField.Name,
                      Result.Name);
    yield return Case(ResultRawEnumField.CreatedBy,
                      Result.CreatedBy);
    yield return Case(ResultRawEnumField.OwnerTaskId,
                      Result.OwnerTaskId);
    yield return Case(ResultRawEnumField.ResultId,
                      Result.ResultId);
    yield return Case(ResultRawEnumField.SessionId,
                      Result.SessionId);
    yield return Case(ResultRawEnumField.Size,
                      Result.Size);
    yield return Case(ResultRawEnumField.OpaqueId,
                      Result.OpaqueId);
    yield return Case(ResultRawEnumField.ManualDeletion,
                      Result.ManualDeletion);
  }

  private static TestCaseData Case(ResultRawEnumField field,
                                   object?            expected)
    => new TestCaseData(new ResultRawField
                        {
                          Field = field,
                        },
                        expected).SetArgDisplayNames(field.ToString());

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(ResultRawField field,
                                              object?        expected)
  {
    var func = new ListResultsRequest
               {
                 Filters = new Filters(),
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field = new ResultField
                                  {
                                    ResultRawField = field,
                                  },
                          Direction = SortDirection.Asc,
                        },
               }.Sort.ToField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(Result));
  }
}
