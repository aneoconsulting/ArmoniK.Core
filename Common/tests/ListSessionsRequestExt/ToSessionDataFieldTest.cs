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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

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

  private static readonly SessionData SessionData = new("SessionId",
                                                        SessionStatus.Running,
                                                        true,
                                                        true,
                                                        DateTime.UtcNow,
                                                        DateTime.UtcNow,
                                                        DateTime.UtcNow,
                                                        DateTime.UtcNow,
                                                        DateTime.UtcNow,
                                                        TimeSpan.FromDays(2),
                                                        new List<string>(),
                                                        Options);

  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    // TODO add Duration
    yield return Case(SessionRawEnumField.Status,
                      SessionData.Status);
    yield return Case(SessionRawEnumField.Options,
                      SessionData.Options);
    yield return Case(SessionRawEnumField.CancelledAt,
                      SessionData.CancellationDate);
    yield return Case(SessionRawEnumField.SessionId,
                      SessionData.SessionId);
    yield return Case(SessionRawEnumField.CreatedAt,
                      SessionData.CreationDate);
    yield return Case(SessionRawEnumField.PartitionIds,
                      SessionData.PartitionIds);
  }

  private static TestCaseData Case(SessionRawEnumField field,
                                   object?             expected)
    => new TestCaseData(new SessionRawField
                        {
                          Field = field,
                        },
                        expected).SetArgDisplayNames(field.ToString());

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(SessionRawField field,
                                              object?         expected)
  {
    var func = new ListSessionsRequest
               {
                 Filters = new Filters(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field = new SessionField
                                  {
                                    SessionRawField = field,
                                  },
                        },
               }.Sort.ToField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(SessionData));
  }
}
