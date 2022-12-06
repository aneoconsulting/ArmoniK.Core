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

using System;
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task metadata stored in database
/// </summary>
/// <param name="SessionId">Unique identifier of the session in which the task belongs</param>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="OwnerPodId">Identifier of the polling agent running the task</param>
/// <param name="PayloadId">Unique identifier of the payload in input of the task</param>
/// <param name="ParentTaskIds">
///   Unique identifiers of the tasks that submitted the current task up to the session id which
///   represents a submission from the client
/// </param>
/// <param name="DataDependencies">Unique identifiers of the results the task depends on</param>
/// <param name="ExpectedOutputIds">
///   Identifiers of the outputs the task should produce or should transmit the
///   responsibility to produce
/// </param>
/// <param name="InitialTaskId">Task id before retry</param>
/// <param name="RetryOfIds">List of previous tasks ids before the current retry</param>
/// <param name="Status">Current status of the task</param>
/// <param name="StatusMessage">Message associated to the status</param>
/// <param name="Options">Task options</param>
/// <param name="CreationDate">Date when the task is created</param>
/// <param name="SubmittedDate">Date when the task is submitted</param>
/// <param name="StartDate">Date when the task execution begins</param>
/// <param name="EndDate">Date when the task ends</param>
/// <param name="ReceptionDate">Date when the task is received by the polling agent</param>
/// <param name="AcquisitionDate">Date when the task is acquired by the pollster</param>
/// <param name="PodTtl">Task Time To Live on the current pod</param>
/// <param name="Output">Output of the task after its successful completion</param>
public record TaskData(string        SessionId,
                       string        TaskId,
                       string        OwnerPodId,
                       string        OwnerPodName,
                       string        PayloadId,
                       IList<string> ParentTaskIds,
                       IList<string> DataDependencies,
                       IList<string> ExpectedOutputIds,
                       string        InitialTaskId,
                       IList<string> RetryOfIds,
                       TaskStatus    Status,
                       string        StatusMessage,
                       TaskOptions   Options,
                       DateTime      CreationDate,
                       DateTime?     SubmittedDate,
                       DateTime?     StartDate,
                       DateTime?     EndDate,
                       DateTime?     ReceptionDate,
                       DateTime?     AcquisitionDate,
                       DateTime?     PodTtl,
                       Output        Output)
{
  /// <summary>
  ///   Initializes task metadata with specified fields
  /// </summary>
  /// <param name="sessionId">Unique identifier of the session in which the task belongs</param>
  /// <param name="taskId">Unique identifier of the task</param>
  /// <param name="ownerPodId">Identifier of the polling agent running the task</param>
  /// <param name="ownerPodName">Hostname of the polling agent running the task</param>
  /// <param name="payloadId">Unique identifier of the payload in input of the task</param>
  /// <param name="parentTaskIds">
  ///   Unique identifiers of the tasks that submitted the current task up to the session id which
  ///   represents a submission from the client
  /// </param>
  /// <param name="dataDependencies">Unique identifiers of the results the task depends on</param>
  /// <param name="expectedOutputIds">
  ///   Identifiers of the outputs the task should produce or should transmit the
  ///   responsibility to produce
  /// </param>
  /// <param name="retryOfIds">List of previous tasks ids before the current retry</param>
  /// <param name="status">Current status of the task</param>
  /// <param name="options">Task options</param>
  /// <param name="output">Output of the task after its successful completion</param>
  public TaskData(string        sessionId,
                  string        taskId,
                  string        ownerPodId,
                  string        ownerPodName,
                  string        payloadId,
                  IList<string> parentTaskIds,
                  IList<string> dataDependencies,
                  IList<string> expectedOutputIds,
                  IList<string> retryOfIds,
                  TaskStatus    status,
                  TaskOptions   options,
                  Output        output)
    : this(sessionId,
           taskId,
           ownerPodId,
           ownerPodName,
           payloadId,
           parentTaskIds,
           dataDependencies,
           expectedOutputIds,
           taskId,
           retryOfIds,
           status,
           "",
           options,
           DateTime.UtcNow,
           null,
           null,
           null,
           null,
           null,
           null,
           output)
  {
  }


  /// <summary>
  ///   Conversion operator from <see cref="TaskData" /> to <see cref="TaskRaw" />
  /// </summary>
  /// <param name="taskData">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static implicit operator TaskRaw(TaskData taskData)
    => new()
       {
         SessionId  = taskData.SessionId,
         Status     = taskData.Status,
         Output     = taskData.Output,
         OwnerPodId = taskData.OwnerPodId,
         Options    = taskData.Options,
         DataDependencies =
         {
           taskData.DataDependencies,
         },
         CreatedAt = FromDateTime(taskData.CreationDate),
         EndedAt = taskData.EndDate is not null
                     ? FromDateTime(taskData.EndDate.Value)
                     : null,
         ExpectedOutputIds =
         {
           taskData.ExpectedOutputIds,
         },
         Id = taskData.TaskId,
         RetryOfIds =
         {
           taskData.RetryOfIds,
         },
         ParentTaskIds =
         {
           taskData.ParentTaskIds,
         },
         PodTtl = taskData.PodTtl is not null
                    ? FromDateTime(taskData.PodTtl.Value)
                    : null,
         StartedAt = taskData.StartDate is not null
                       ? FromDateTime(taskData.StartDate.Value)
                       : null,
         StatusMessage = taskData.StatusMessage,
         SubmittedAt = taskData.SubmittedDate is not null
                         ? FromDateTime(taskData.SubmittedDate.Value)
                         : null,
         AcquiredAt = taskData.AcquisitionDate is not null
                        ? FromDateTime(taskData.AcquisitionDate.Value)
                        : null,
         ReceivedAt = taskData.ReceptionDate is not null
                        ? FromDateTime(taskData.ReceptionDate.Value)
                        : null,
         PodHostname = taskData.OwnerPodName,
       };

  /// <summary>
  ///   Conversion operator from <see cref="TaskData" /> to <see cref="TaskSummary" />
  /// </summary>
  /// <param name="taskData">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static implicit operator TaskSummary(TaskData taskData)
    => new()
       {
         SessionId = taskData.SessionId,
         Status    = taskData.Status,
         Options   = taskData.Options,
         CreatedAt = FromDateTime(taskData.CreationDate),
         EndedAt = taskData.EndDate is not null
                     ? FromDateTime(taskData.EndDate.Value)
                     : null,
         Id = taskData.TaskId,

         StartedAt = taskData.StartDate is not null
                       ? FromDateTime(taskData.StartDate.Value)
                       : null,
         Error = taskData.Status == TaskStatus.Error
                   ? taskData.Output.Error
                   : "",
       };

  /// <summary>
  ///   Conversion operator from <see cref="TaskData" /> to <see cref="Application" />
  /// </summary>
  /// <param name="taskData">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static implicit operator Application(TaskData taskData)
    => new(taskData.Options.ApplicationName,
           taskData.Options.ApplicationNamespace,
           taskData.Options.ApplicationVersion,
           taskData.Options.ApplicationService);
}
