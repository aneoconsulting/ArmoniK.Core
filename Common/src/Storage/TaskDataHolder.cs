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

using ArmoniK.Core.Base.DataStructures;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task metadata place holder for dynamic conversions
/// </summary>
[UsedImplicitly]
public record TaskDataHolder
{
  /// <summary>
  ///   Unique identifier of the task
  /// </summary>
  public string TaskId { get; set; } = string.Empty;

  /// <summary>
  ///   Unique identifier of the session in which the task belongs
  /// </summary>
  public string SessionId { get; set; } = string.Empty;

  /// <summary>
  ///   Identifier of the polling agent running the task
  /// </summary>
  public string OwnerPodId { get; set; } = string.Empty;

  /// <summary>
  ///   Name of the polling agent running the task
  /// </summary>
  public string OwnerPodName { get; set; } = string.Empty;

  /// <summary>
  ///   Unique identifier of the payload in input of the task
  /// </summary>
  public string PayloadId { get; set; } = string.Empty;

  /// <summary>
  ///   Task id before retry
  /// </summary>
  public string InitialTaskId { get; set; } = string.Empty;

  /// <summary>
  ///   Message associated to the status
  /// </summary>
  public string StatusMessage { get; set; } = string.Empty;

  /// <summary>
  ///   Unique identifiers of the results the task depends on
  /// </summary>
  public IList<string> DataDependencies { get; set; } = Array.Empty<string>();

  /// <summary>
  ///   Unique identifiers of the tasks that submitted the current task up to the session id which
  ///   represents a submission from the client
  /// </summary>
  public IList<string> ParentTaskIds { get; set; } = Array.Empty<string>();

  /// <summary>
  ///   Identifiers of the outputs the task should produce or should transmit the
  ///   responsibility to produce
  /// </summary>
  public IList<string> ExpectedOutputIds { get; set; } = Array.Empty<string>();

  /// <summary>
  ///   List of previous tasks ids before the current retry
  /// </summary>
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

  /// <summary>
  ///   Date when the task is created
  /// </summary>
  public DateTime? CreationDate { get; set; } = null;

  /// <summary>
  ///   Date when the task is submitted
  /// </summary>
  public DateTime? SubmittedDate { get; set; } = null;

  /// <summary>
  ///   Date when the task execution begins
  /// </summary>
  public DateTime? StartDate { get; set; } = null;

  /// <summary>
  ///   Date when the task ends
  /// </summary>
  public DateTime? EndDate { get; set; } = null;

  /// <summary>
  ///   Date when the task is received by the polling agent
  /// </summary>
  public DateTime? ReceptionDate { get; set; } = null;

  /// <summary>
  ///   Date when the task is acquired by the pollster
  /// </summary>
  public DateTime? AcquisitionDate { get; set; } = null;

  /// <summary>
  ///   Duration between the start of processing and the end of the task
  /// </summary>
  public TimeSpan? ProcessingToEndDuration { get; set; } = null;

  /// <summary>
  ///   Duration between the creation and the end of the task
  /// </summary>
  public TimeSpan? CreationToEndDuration { get; set; } = null;

  /// <summary>
  ///   Task Time To Live on the current pod
  /// </summary>
  public DateTime? PodTtl { get; set; } = null;

  /// <summary>
  ///   Output of the task after its successful completion
  /// </summary>
  public Output? Output { get; set; } = null;

  /// <summary>
  ///   Task options
  /// </summary>
  public TaskOptions? Options { get; set; } = null;

  /// <summary>
  ///   Current status of the task
  /// </summary>
  public TaskStatus Status { get; set; } = TaskStatus.Unspecified;
}
