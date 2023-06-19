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

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListTasksRequestExt;

[TestFixture(TestOf = typeof(ToTaskDataFieldTest))]
public class ToTaskDataFieldTest
{
  private static readonly TaskOptions Options = new(new Dictionary<string, string>
                                                    {
                                                      {
                                                        "key1", "value1"
                                                      },
                                                      {
                                                        "key2", "value2"
                                                      },
                                                    },
                                                    TimeSpan.MaxValue,
                                                    5,
                                                    1,
                                                    "part1",
                                                    "applicationName",
                                                    "applicationVersion",
                                                    "applicationNamespace",
                                                    "applicationService",
                                                    "engineType");

  private static readonly TaskData TaskData = new("SessionId",
                                                  "TaskCompletedId",
                                                  "OwnerPodId",
                                                  "OwnerPodName",
                                                  "PayloadId",
                                                  new[]
                                                  {
                                                    "parent1",
                                                  },
                                                  new[]
                                                  {
                                                    "dependency1",
                                                  },
                                                  new[]
                                                  {
                                                    "output1",
                                                  },
                                                  Array.Empty<string>(),
                                                  TaskStatus.Completed,
                                                  Options,
                                                  new Output(true,
                                                             ""));

  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    TestCaseData CaseSummary(TaskSummaryField field,
                             object?          expected)
      => new TestCaseData(new TaskField
                          {
                            TaskSummaryField = field,
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return CaseSummary(TaskSummaryField.TaskId,
                             TaskData.TaskId);
    yield return CaseSummary(TaskSummaryField.SessionId,
                             TaskData.SessionId);
    yield return CaseSummary(TaskSummaryField.Status,
                             TaskData.Status);
    yield return CaseSummary(TaskSummaryField.CreatedAt,
                             TaskData.CreationDate);
    yield return CaseSummary(TaskSummaryField.StartedAt,
                             TaskData.StartDate);
    yield return CaseSummary(TaskSummaryField.EndedAt,
                             TaskData.EndDate);
    yield return CaseSummary(TaskSummaryField.CreationToEndDuration,
                             TaskData.CreationToEndDuration);
    yield return CaseSummary(TaskSummaryField.ProcessingToEndDuration,
                             TaskData.ProcessingToEndDuration);
    yield return CaseSummary(TaskSummaryField.Error,
                             TaskData.Output.Error);
    yield return CaseSummary(TaskSummaryField.OwnerPodId,
                             TaskData.OwnerPodId);
    yield return CaseSummary(TaskSummaryField.InitialTaskId,
                             TaskData.InitialTaskId);
    yield return CaseSummary(TaskSummaryField.SubmittedAt,
                             TaskData.SubmittedDate);
    yield return CaseSummary(TaskSummaryField.PodTtl,
                             TaskData.PodTtl);
    yield return CaseSummary(TaskSummaryField.PodHostname,
                             TaskData.OwnerPodName);
    yield return CaseSummary(TaskSummaryField.ReceivedAt,
                             TaskData.ReceptionDate);
    yield return CaseSummary(TaskSummaryField.AcquiredAt,
                             TaskData.AcquisitionDate);

    TestCaseData CaseOption(TaskOptionField field,
                            object?         expected)
      => new TestCaseData(new TaskField
                          {
                            TaskOptionField = field,
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return CaseOption(TaskOptionField.MaxDuration,
                            TaskData.Options.MaxDuration);
    yield return CaseOption(TaskOptionField.MaxRetries,
                            TaskData.Options.MaxRetries);
    yield return CaseOption(TaskOptionField.Priority,
                            TaskData.Options.Priority);
    yield return CaseOption(TaskOptionField.PartitionId,
                            TaskData.Options.PartitionId);
    yield return CaseOption(TaskOptionField.ApplicationName,
                            TaskData.Options.ApplicationName);
    yield return CaseOption(TaskOptionField.ApplicationVersion,
                            TaskData.Options.ApplicationVersion);
    yield return CaseOption(TaskOptionField.ApplicationNamespace,
                            TaskData.Options.ApplicationNamespace);
    yield return CaseOption(TaskOptionField.ApplicationService,
                            TaskData.Options.ApplicationService);
    yield return CaseOption(TaskOptionField.EngineType,
                            TaskData.Options.EngineType);

    TestCaseData CaseOptionGeneric(string key)
      => new TestCaseData(new TaskField
                          {
                            TaskOptionGenericField = new TaskOptionGenericField
                                                     {
                                                       Field = key,
                                                     },
                          },
                          TaskData.Options.Options[key]).SetArgDisplayNames(key);

    yield return CaseOptionGeneric("key1");
    yield return CaseOptionGeneric("key2");
  }

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(TaskField field,
                                              object?   expected)
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field     = field,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(TaskData));
  }

  [Test]
  public void KeyNotFoundShouldFail()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field = new TaskField
                                  {
                                    TaskOptionGenericField = new TaskOptionGenericField
                                                             {
                                                               Field = "NotExistingKey",
                                                             },
                                  },
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.Throws<KeyNotFoundException>(() => func.Invoke(TaskData));
  }
}
