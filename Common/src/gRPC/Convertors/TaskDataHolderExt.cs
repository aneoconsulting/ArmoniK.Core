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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class TaskDataHolderExt
{
  /// <summary>
  ///   Conversion operator from <see cref="TaskDataHolder" /> to <see cref="TaskDetailed" />
  /// </summary>
  /// <param name="taskData">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static TaskDetailed ToTaskDetailed(this TaskDataHolder taskData)
    => new()
       {
         SessionId = taskData.SessionId,
         PayloadId = taskData.PayloadId,
         Status    = taskData.Status.ToGrpcStatus(),
         Output = taskData.Output is not null
                    ? new TaskDetailed.Types.Output
                      {
                        Error   = taskData.Output.Error,
                        Success = taskData.Output.Status == OutputStatus.Success,
                      }
                    : null,
         OwnerPodId = taskData.OwnerPodId,
         Options    = taskData.Options?.ToGrpcTaskOptions(),
         DataDependencies =
         {
           taskData.DataDependencies,
         },
         CreatedAt = taskData.CreationDate is not null
                       ? FromDateTime(taskData.CreationDate.Value)
                       : null,
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
         CreationToEndDuration = taskData.CreationToEndDuration is not null
                                   ? Duration.FromTimeSpan(taskData.CreationToEndDuration.Value)
                                   : null,
         ProcessingToEndDuration = taskData.ProcessingToEndDuration is not null
                                     ? Duration.FromTimeSpan(taskData.ProcessingToEndDuration.Value)
                                     : null,
         InitialTaskId = taskData.InitialTaskId,
         ReceivedToEndDuration = taskData.ReceivedToEndDuration is not null
                                   ? Duration.FromTimeSpan(taskData.ReceivedToEndDuration.Value)
                                   : null,
         ProcessedAt = taskData.ProcessedDate is not null
                         ? FromDateTime(taskData.ProcessedDate.Value)
                         : null,
         FetchedAt = taskData.FetchedDate is not null
                       ? FromDateTime(taskData.FetchedDate.Value)
                       : null,
       };


  /// <summary>
  ///   Conversion operator from <see cref="TaskDataHolder" /> to gRPC <see cref="TaskSummary" />
  /// </summary>
  /// <param name="taskDataSummary">The input task data</param>
  /// <returns>
  ///   The converted task data
  /// </returns>
  public static TaskSummary ToTaskSummary(this TaskDataHolder taskDataSummary)
    => new()
       {
         SessionId  = taskDataSummary.SessionId,
         PayloadId  = taskDataSummary.PayloadId,
         Status     = taskDataSummary.Status.ToGrpcStatus(),
         OwnerPodId = taskDataSummary.OwnerPodId,
         Options    = taskDataSummary.Options?.ToGrpcTaskOptions(),
         CreatedAt = taskDataSummary.CreationDate is not null
                       ? FromDateTime(taskDataSummary.CreationDate.Value)
                       : null,
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
                   ? taskDataSummary.Output?.Error
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
         ReceivedToEndDuration = taskDataSummary.ReceivedToEndDuration is not null
                                   ? Duration.FromTimeSpan(taskDataSummary.ReceivedToEndDuration.Value)
                                   : null,
         ProcessedAt = taskDataSummary.ProcessedDate is not null
                         ? FromDateTime(taskDataSummary.ProcessedDate.Value)
                         : null,
         FetchedAt = taskDataSummary.FetchedDate is not null
                       ? FromDateTime(taskDataSummary.FetchedDate.Value)
                       : null,
       };
}
