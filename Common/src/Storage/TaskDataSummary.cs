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

using ArmoniK.Api.gRPC.V1;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task metadata summary
/// </summary>
/// <param name="SessionId">Unique identifier of the session in which the task belongs</param>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="OwnerPodId">Identifier of the polling agent running the task</param>
/// <param name="ParentTaskIdsCount">
///   Count of the tasks that submitted the current task up to the session id which
///   represents a submission from the client
/// </param>
/// <param name="DataDependenciesCount">Count of identifiers of the results the task depends on</param>
/// <param name="ExpectedOutputIdsCount">
///   Count of the outputs the task should produce or should transmit the
///   responsibility to produce
/// </param>
/// <param name="InitialTaskId">Task id before retry</param>
/// <param name="RetryOfIdsCount">Count of the previous tasks ids before the current retry</param>
/// <param name="Status">Current status of the task</param>
/// <param name="StatusMessage">Message associated to the status</param>
/// <param name="Options">Task options</param>
/// <param name="CreationDate">Date when the task is created</param>
/// <param name="SubmittedDate">Date when the task is submitted</param>
/// <param name="StartDate">Date when the task execution begins</param>
/// <param name="EndDate">Date when the task ends</param>
/// <param name="ReceptionDate">Date when the task is received by the polling agent</param>
/// <param name="AcquisitionDate">Date when the task is acquired by the pollster</param>
/// <param name="ProcessingToEndDuration">Duration between the start of processing and the end of the task</param>
/// <param name="CreationToEndDuration">Duration between the creation and the end of the task</param>
/// <param name="PodTtl">Task Time To Live on the current pod</param>
/// <param name="Output">Output of the task after its successful completion</param>
public record TaskDataSummary(string      SessionId,
                              string      TaskId,
                              string      OwnerPodId,
                              string      OwnerPodName,
                              int         ParentTaskIdsCount,
                              int         DataDependenciesCount,
                              int         ExpectedOutputIdsCount,
                              string      InitialTaskId,
                              int         RetryOfIdsCount,
                              TaskStatus  Status,
                              string      StatusMessage,
                              TaskOptions Options,
                              DateTime    CreationDate,
                              DateTime?   SubmittedDate,
                              DateTime?   StartDate,
                              DateTime?   EndDate,
                              DateTime?   ReceptionDate,
                              DateTime?   AcquisitionDate,
                              DateTime?   PodTtl,
                              TimeSpan?   ProcessingToEndDuration,
                              TimeSpan?   CreationToEndDuration,
                              Output      Output);
