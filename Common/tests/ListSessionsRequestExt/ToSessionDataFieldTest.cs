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
using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

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
                                                        DateTime.UtcNow,
                                                        DateTime.UtcNow,
                                                        new List<string>(),
                                                        Options);

  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    TestCaseData Case(SessionRawField field,
                      object?         expected)
      => new TestCaseData(field,
                          expected).SetArgDisplayNames(field.ToString());

    // TODO add Duration
    yield return Case(SessionRawField.Status,
                      SessionData.Status);
    yield return Case(SessionRawField.Options,
                      SessionData.Options);
    yield return Case(SessionRawField.CancelledAt,
                      SessionData.CancellationDate);
    yield return Case(SessionRawField.SessionId,
                      SessionData.SessionId);
    yield return Case(SessionRawField.CreatedAt,
                      SessionData.CreationDate);
    yield return Case(SessionRawField.PartitionIds,
                      SessionData.PartitionIds);
  }

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(SessionRawField field,
                                              object?         expected)
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter(),
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field = new SessionField
                                  {
                                    SessionRawField = field,
                                  },
                        },
               }.Sort.ToSessionDataField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(SessionData));
  }
}
