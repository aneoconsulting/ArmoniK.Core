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

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Fields available in <see cref="TaskDataHolder" />
/// </summary>
public enum TaskDataFields
{
  /// <inheritdoc cref="TaskDataHolder.SessionId" />
  SessionId,

  /// <inheritdoc cref="TaskDataHolder.TaskId" />
  TaskId,

  /// <inheritdoc cref="TaskDataHolder.PayloadId" />
  PayloadId,

  /// <inheritdoc cref="TaskDataHolder.ParentTaskIds" />
  ParentTaskIds,

  /// <inheritdoc cref="TaskDataHolder.ParentTaskIdsCount" />
  ParentTaskIdsCount,

  /// <inheritdoc cref="TaskDataHolder.ExpectedOutputIds" />
  ExpectedOutputIds,

  /// <inheritdoc cref="TaskDataHolder.ExpectedOutputIdsCount" />
  ExpectedOutputIdsCount,

  /// <inheritdoc cref="TaskDataHolder.InitialTaskId" />
  InitialTaskId,

  /// <inheritdoc cref="TaskDataHolder.RetryOfIds" />
  RetryOfIds,

  /// <inheritdoc cref="TaskDataHolder.RetryOfIdsCount" />
  RetryOfIdsCount,

  /// <inheritdoc cref="TaskDataHolder.Status" />
  Status,

  /// <inheritdoc cref="TaskDataHolder.Options" />
  Options,

  /// <inheritdoc cref="TaskDataHolder.DataDependencies" />
  DataDependencies,

  /// <inheritdoc cref="TaskDataHolder.DataDependenciesCount" />
  DataDependenciesCount,

  /// <inheritdoc cref="TaskDataHolder.OwnerPodId" />
  OwnerPodId,

  /// <inheritdoc cref="TaskDataHolder.OwnerPodName" />
  OwnerPodName,

  /// <inheritdoc cref="TaskDataHolder.StatusMessage" />
  StatusMessage,

  /// <inheritdoc cref="TaskDataHolder.CreationDate" />
  CreationDate,

  /// <inheritdoc cref="TaskDataHolder.SubmittedDate" />
  SubmittedDate,

  /// <inheritdoc cref="TaskDataHolder.StartDate" />
  StartDate,

  /// <inheritdoc cref="TaskDataHolder.EndDate" />
  EndDate,

  /// <inheritdoc cref="TaskDataHolder.ReceptionDate" />
  ReceptionDate,

  /// <inheritdoc cref="TaskDataHolder.AcquisitionDate" />
  AcquisitionDate,

  /// <inheritdoc cref="TaskDataHolder.ProcessedDate" />
  ProcessedDate,

  /// <inheritdoc cref="TaskDataHolder.ProcessingToEndDuration" />
  ProcessingToEndDuration,

  /// <inheritdoc cref="TaskDataHolder.CreationToEndDuration" />
  CreationToEndDuration,

  /// <inheritdoc cref="TaskDataHolder.ReceivedToEndDuration" />
  ReceivedToEndDuration,

  /// <inheritdoc cref="TaskDataHolder.PodTtl" />
  PodTtl,

  /// <inheritdoc cref="TaskDataHolder.Output" />
  Output,
}
