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

using Armonik.Api.gRPC.V1.Tasks;

namespace ArmoniK.Core.Common.Storage;

public record TaskData(string        SessionId,
                       string        TaskId,
                       string        OwnerPodId,
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
                       DateTime?     PodTtl,
                       Output        Output)
{
  public TaskData(string        sessionId,
                  string        taskId,
                  string        ownerPodId,
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
           output)
  {
  }

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
         CreatedAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.CreationDate),
         EndedAt = taskData.EndDate is not null
                     ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.EndDate.Value)
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
                    ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.PodTtl.Value)
                    : null,
         StartedAt = taskData.StartDate is not null
                       ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.StartDate.Value)
                       : null,
         StatusMessage = taskData.StatusMessage,
         SubmittedAt = taskData.SubmittedDate is not null
                         ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.SubmittedDate.Value)
                         : null,
       };

  public static implicit operator Task(TaskData taskData)
    => new()
       {
         SessionId = taskData.SessionId,
         Status    = taskData.Status,
         Options   = taskData.Options,
         CreatedAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.CreationDate),
         EndedAt = taskData.EndDate is not null
                     ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.EndDate.Value)
                     : null,
         Id = taskData.TaskId,

         StartedAt = taskData.StartDate is not null
                       ? Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(taskData.StartDate.Value)
                       : null,
       };
}
