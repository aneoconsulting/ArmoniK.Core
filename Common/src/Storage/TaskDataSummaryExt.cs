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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC.Convertors;

using Google.Protobuf.WellKnownTypes;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Storage;

public static class TaskDataSummaryExt
{
  /// <summary>
  ///   Conversion operator from <see cref="TaskDataSummary" /> to gRPC <see cref="TaskSummary" />
  /// </summary>
  /// <param name="taskDataSummary">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static TaskSummary ToTaskSummary(this TaskDataSummary taskDataSummary)
    => new()
       {
         SessionId  = taskDataSummary.SessionId,
         Status     = taskDataSummary.Status.ToGrpcStatus(),
         OwnerPodId = taskDataSummary.OwnerPodId,
         Options    = taskDataSummary.Options.ToGrpcTaskOptions(),
         CreatedAt  = FromDateTime(taskDataSummary.CreationDate),
         EndedAt = taskDataSummary.EndDate is not null
                     ? FromDateTime(taskDataSummary.EndDate.Value)
                     : null,
         Id = taskDataSummary.TaskId,
         PodTtl = taskDataSummary.PodTtl is not null
                    ? FromDateTime(taskDataSummary.PodTtl.Value)
                    : null,
         StartedAt = taskDataSummary.StartDate is not null
                       ? FromDateTime(taskDataSummary.StartDate.Value)
                       : null,
         Error = taskDataSummary.Status == TaskStatus.Error
                   ? taskDataSummary.Output.Error
                   : "",
         StatusMessage = taskDataSummary.StatusMessage,
         SubmittedAt = taskDataSummary.SubmittedDate is not null
                         ? FromDateTime(taskDataSummary.SubmittedDate.Value)
                         : null,
         AcquiredAt = taskDataSummary.AcquisitionDate is not null
                        ? FromDateTime(taskDataSummary.AcquisitionDate.Value)
                        : null,
         ReceivedAt = taskDataSummary.ReceptionDate is not null
                        ? FromDateTime(taskDataSummary.ReceptionDate.Value)
                        : null,
         PodHostname = taskDataSummary.OwnerPodName,
         CreationToEndDuration = taskDataSummary.CreationToEndDuration is not null
                                   ? Duration.FromTimeSpan(taskDataSummary.CreationToEndDuration.Value)
                                   : null,
         ProcessingToEndDuration = taskDataSummary.ProcessingToEndDuration is not null
                                     ? Duration.FromTimeSpan(taskDataSummary.ProcessingToEndDuration.Value)
                                     : null,
         InitialTaskId          = taskDataSummary.InitialTaskId,
         CountDataDependencies  = taskDataSummary.DataDependenciesCount,
         CountExpectedOutputIds = taskDataSummary.ExpectedOutputIdsCount,
         CountParentTaskIds     = taskDataSummary.ParentTaskIdsCount,
         CountRetryOfIds        = taskDataSummary.RetryOfIdsCount,
       };
}
