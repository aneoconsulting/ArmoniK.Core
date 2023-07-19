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

using Armonik.Api.gRPC.V1;

using ArmoniK.Api.gRPC.V1;

using Armonik.Api.Grpc.V1.SortDirection;
using Armonik.Api.gRPC.V1.Tasks;

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
    TestCaseData CaseSummary(TaskSummaryEnumField field,
                             object?              expected)
      => new TestCaseData(new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = field,
                                               },
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return CaseSummary(TaskSummaryEnumField.TaskId,
                             TaskData.TaskId);
    yield return CaseSummary(TaskSummaryEnumField.SessionId,
                             TaskData.SessionId);
    yield return CaseSummary(TaskSummaryEnumField.Status,
                             TaskData.Status);
    yield return CaseSummary(TaskSummaryEnumField.CreatedAt,
                             TaskData.CreationDate);
    yield return CaseSummary(TaskSummaryEnumField.StartedAt,
                             TaskData.StartDate);
    yield return CaseSummary(TaskSummaryEnumField.EndedAt,
                             TaskData.EndDate);
    yield return CaseSummary(TaskSummaryEnumField.CreationToEndDuration,
                             TaskData.CreationToEndDuration);
    yield return CaseSummary(TaskSummaryEnumField.ProcessingToEndDuration,
                             TaskData.ProcessingToEndDuration);
    yield return CaseSummary(TaskSummaryEnumField.Error,
                             TaskData.Output.Error);
    yield return CaseSummary(TaskSummaryEnumField.OwnerPodId,
                             TaskData.OwnerPodId);
    yield return CaseSummary(TaskSummaryEnumField.InitialTaskId,
                             TaskData.InitialTaskId);
    yield return CaseSummary(TaskSummaryEnumField.SubmittedAt,
                             TaskData.SubmittedDate);
    yield return CaseSummary(TaskSummaryEnumField.PodTtl,
                             TaskData.PodTtl);
    yield return CaseSummary(TaskSummaryEnumField.PodHostname,
                             TaskData.OwnerPodName);
    yield return CaseSummary(TaskSummaryEnumField.ReceivedAt,
                             TaskData.ReceptionDate);
    yield return CaseSummary(TaskSummaryEnumField.AcquiredAt,
                             TaskData.AcquisitionDate);

    TestCaseData CaseOption(TaskOptionEnumField field,
                            object?             expected)
      => new TestCaseData(new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = field,
                                              },
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return CaseOption(TaskOptionEnumField.MaxDuration,
                            TaskData.Options.MaxDuration);
    yield return CaseOption(TaskOptionEnumField.MaxRetries,
                            TaskData.Options.MaxRetries);
    yield return CaseOption(TaskOptionEnumField.Priority,
                            TaskData.Options.Priority);
    yield return CaseOption(TaskOptionEnumField.PartitionId,
                            TaskData.Options.PartitionId);
    yield return CaseOption(TaskOptionEnumField.ApplicationName,
                            TaskData.Options.ApplicationName);
    yield return CaseOption(TaskOptionEnumField.ApplicationVersion,
                            TaskData.Options.ApplicationVersion);
    yield return CaseOption(TaskOptionEnumField.ApplicationNamespace,
                            TaskData.Options.ApplicationNamespace);
    yield return CaseOption(TaskOptionEnumField.ApplicationService,
                            TaskData.Options.ApplicationService);
    yield return CaseOption(TaskOptionEnumField.EngineType,
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
                                                             Field = new TaskField
                                                                     {
                                                                       TaskSummaryField = new TaskSummaryField
                                                                                          {
                                                                                            Field = TaskSummaryEnumField.SessionId,
                                                                                          },
                                                                     },
                                                             Value    = "SessionId",
                                                             Operator = FilterStringOperator.Equal,
                                                           },
                                                },
                                              },
                                            },
                                          },
                                        },
                           },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = SortDirection.Asc,
                          Field     = field,
                        },
               }.Sort.ToField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(TaskData));
  }

  [Test]
  public void KeyNotFoundShouldFail()
  {
    var func = new ListTasksRequest
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
                                                             Field = new TaskField
                                                                     {
                                                                       TaskSummaryField = new TaskSummaryField
                                                                                          {
                                                                                            Field = TaskSummaryEnumField.SessionId,
                                                                                          },
                                                                     },
                                                             Value    = "SessionId",
                                                             Operator = FilterStringOperator.Equal,
                                                           },
                                                },
                                              },
                                            },
                                          },
                                        },
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
               }.Sort.ToField()
                .Compile();

    Assert.Throws<KeyNotFoundException>(() => func.Invoke(TaskData));
  }
}
