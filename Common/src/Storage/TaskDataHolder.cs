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

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task metadata place holder for dynamic conversions
/// </summary>
[UsedImplicitly]
public record TaskDataHolder
{
  /// <inheritdoc cref="TaskData.TaskId" />
  public string TaskId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.SessionId" />
  public string SessionId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.OwnerPodId" />
  public string OwnerPodId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.OwnerPodName" />
  public string OwnerPodName { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.PayloadId" />
  public string PayloadId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.InitialTaskId" />
  public string InitialTaskId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.CreatedBy" />
  public string CreatedBy { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.StatusMessage" />
  public string StatusMessage { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskData.DataDependencies" />
  public IList<string> DataDependencies { get; set; } = Array.Empty<string>();

  /// <inheritdoc cref="TaskData.ParentTaskIds" />
  public IList<string> ParentTaskIds { get; set; } = Array.Empty<string>();

  /// <inheritdoc cref="TaskData.ExpectedOutputIds" />
  public IList<string> ExpectedOutputIds { get; set; } = Array.Empty<string>();

  /// <inheritdoc cref="TaskData.RetryOfIds" />
  public IList<string> RetryOfIds { get; set; } = Array.Empty<string>();

  /// <summary>
  ///   Count of the results the task depends on
  /// </summary>
  public int DataDependenciesCount { get; set; } = 0;

  /// <summary>
  ///   Count of parent task ids
  /// </summary>
  public int ParentTaskIdsCount { get; set; } = 0;

  /// <summary>
  ///   Count of expected output ids
  /// </summary>
  public int ExpectedOutputIdsCount { get; set; } = 0;

  /// <summary>
  ///   Count of previous tasks ids before the current retry
  /// </summary>
  public int RetryOfIdsCount { get; set; } = 0;

  /// <inheritdoc cref="TaskData.CreationDate" />
  public DateTime? CreationDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.SubmittedDate" />
  public DateTime? SubmittedDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.StartDate" />
  public DateTime? StartDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.EndDate" />
  public DateTime? EndDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.ReceptionDate" />
  public DateTime? ReceptionDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.AcquisitionDate" />
  public DateTime? AcquisitionDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.ProcessingToEndDuration" />
  public TimeSpan? ProcessingToEndDuration { get; set; } = null;

  /// <inheritdoc cref="TaskData.CreationToEndDuration" />
  public TimeSpan? CreationToEndDuration { get; set; } = null;

  /// <inheritdoc cref="TaskData.ReceivedToEndDuration" />
  public TimeSpan? ReceivedToEndDuration { get; set; } = null;

  /// <inheritdoc cref="TaskData.PodTtl" />
  public DateTime? PodTtl { get; set; } = null;

  /// <inheritdoc cref="TaskData.ProcessedDate" />
  public DateTime? ProcessedDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.FetchedDate" />
  public DateTime? FetchedDate { get; set; } = null;

  /// <inheritdoc cref="TaskData.Output" />
  public Output? Output { get; set; } = null;

  /// <inheritdoc cref="TaskData.Options" />
  public TaskOptionsHolder? Options { get; set; } = null;

  /// <inheritdoc cref="TaskData.Status" />
  public TaskStatus Status { get; set; } = TaskStatus.Unspecified;
}
