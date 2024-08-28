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
using System.Linq;
using System.Text;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task metadata stored in database
/// </summary>
/// <param name="SessionId">Unique identifier of the session in which the task belongs</param>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="OwnerPodId">Identifier of the polling agent running the task</param>
/// <param name="OwnerPodName">Name of the polling agent running the task</param>
/// <param name="PayloadId">Unique identifier of the payload in input of the task</param>
/// <param name="ParentTaskIds">
///   Unique identifiers of the tasks that submitted the current task up to the session id which
///   represents a submission from the client
/// </param>
/// <param name="DataDependencies">Unique identifiers of the results the task depends on</param>
/// <param name="RemainingDataDependencies">List of dependencies that are not yet satisfied</param>
/// <param name="ExpectedOutputIds">
///   Identifiers of the outputs the task should produce or should transmit the
///   responsibility to produce
/// </param>
/// <param name="InitialTaskId">Task id before retry</param>
/// <param name="CreatedBy">Id of the task that created this task.</param>
/// <param name="RetryOfIds">List of previous tasks ids before the current retry</param>
/// <param name="Status">Current status of the task</param>
/// <param name="StatusMessage">Message associated to the status</param>
/// <param name="Options">Task options</param>
/// <param name="CreationDate">Date when the task is created</param>
/// <param name="SubmittedDate">Date when the task is submitted</param>
/// <param name="FetchedDate">Date when task data are fetched</param>
/// <param name="StartDate">Date when the task execution begins</param>
/// <param name="ProcessedDate">Date when the task execution ends</param>
/// <param name="EndDate">Date when the task ends</param>
/// <param name="ReceptionDate">Date when the task is received by the polling agent</param>
/// <param name="AcquisitionDate">Date when the task is acquired by the pollster</param>
/// <param name="ProcessingToEndDuration">Duration between the start of processing and the end of the task</param>
/// <param name="CreationToEndDuration">Duration between the creation and the end of the task</param>
/// <param name="ReceivedToEndDuration">Duration between the reception and the end of the task</param>
/// <param name="PodTtl">Task Time To Live on the current pod</param>
/// <param name="Output">Output of the task after its successful completion</param>
public record TaskData(string        SessionId,
                       string        TaskId,
                       string        OwnerPodId,
                       string        OwnerPodName,
                       string        PayloadId,
                       IList<string> ParentTaskIds,
                       IList<string> DataDependencies,
                       // FIXME: RemainingDataDependencies should be a HashSet, but there is no HashSet in MongoDB.
                       // List would also work but would make dependency management *much* slower when there is many dependencies on a single task.
                       // (Removing elements from a list is linear time, but removing from an object in constant time)
                       // Ideal solution would most likely be to put HashSet here, and have a custom Serializer/Deserializer in MongoDB "schema".
                       IDictionary<string, bool> RemainingDataDependencies,
                       IList<string>             ExpectedOutputIds,
                       string                    InitialTaskId,
                       string                    CreatedBy,
                       IList<string>             RetryOfIds,
                       TaskStatus                Status,
                       string                    StatusMessage,
                       TaskOptions               Options,
                       DateTime                  CreationDate,
                       DateTime?                 SubmittedDate,
                       DateTime?                 StartDate,
                       DateTime?                 EndDate,
                       DateTime?                 ReceptionDate,
                       DateTime?                 AcquisitionDate,
                       DateTime?                 ProcessedDate,
                       DateTime?                 FetchedDate,
                       DateTime?                 PodTtl,
                       TimeSpan?                 ProcessingToEndDuration,
                       TimeSpan?                 CreationToEndDuration,
                       TimeSpan?                 ReceivedToEndDuration,
                       Output                    Output)
{
  /// <summary>
  ///   Initializes task metadata with specified fields
  /// </summary>
  /// <param name="sessionId">Unique identifier of the session in which the task belongs</param>
  /// <param name="taskId">Unique identifier of the task</param>
  /// <param name="ownerPodId">Identifier of the polling agent running the task</param>
  /// <param name="ownerPodName">Hostname of the polling agent running the task</param>
  /// <param name="payloadId">Unique identifier of the payload in input of the task</param>
  /// <param name="createdBy">Id of the task that created this task.</param>
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
                  string        createdBy,
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
           dataDependencies.Concat(new[]
                                   {
                                     payloadId,
                                   })
                           .ToDictionary(EscapeKey,
                                         _ => true),
           expectedOutputIds,
           taskId,
           createdBy,
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
           null,
           null,
           null,
           null,
           null,
           output)
  {
  }

  /// <summary>
  ///   Creates a copy of a <see cref="TaskData" /> and modify it according to given updates
  /// </summary>
  /// <param name="original">The object that will be copied</param>
  /// <param name="updates">A collection of field selector and their new values</param>
  public TaskData(TaskData                   original,
                  UpdateDefinition<TaskData> updates)
    : this(original)
    => updates.ApplyTo(this);

  /// <summary>
  ///   ResultIds could contain dots (eg: it is the case in htcmock),
  ///   but MongoDB does not support well dots in keys.
  ///   This escapes the key to replace dots with something else.
  ///   Escaped keys are guaranteed to have neither dots nor dollars
  /// </summary>
  /// <param name="key">Key string</param>
  /// <returns>Escaped key</returns>
  public static string EscapeKey(string key)
    => new StringBuilder(key).Replace("@",
                                      "@at@")
                             .Replace(".",
                                      "@dot@")
                             .Replace("$",
                                      "@dollar@")
                             .ToString();

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
