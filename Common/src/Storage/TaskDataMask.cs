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
using System.Linq.Expressions;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Represents a projection from <see cref="TaskData" /> to <see cref="TaskDataHolder" /> with a mask that selects the
///   taskDataFields to include in <see cref="TaskDataHolder" />
/// </summary>
public class TaskDataMask
{
  private readonly Expression<Func<TaskData, TaskDataHolder>> selector_;

  /// <summary>
  ///   Creates an instance of class that holds a projection from <see cref="TaskData" /> to <see cref="TaskDataHolder" />
  ///   with the mask built from the given list of taskDataFields
  /// </summary>
  /// <param name="taskDataFields">Fields from <see cref="TaskData" /> to include in the mask</param>
  /// <param name="taskOptionsFields">Fields from <see cref="TaskOptions" />  to include in the mask</param>
  public TaskDataMask(ICollection<TaskDataFields>    taskDataFields,
                      ICollection<TaskOptionsFields> taskOptionsFields)
  {
    if (!taskOptionsFields.Any())
    {
      taskOptionsFields = Enum.GetValues<TaskOptionsFields>();
    }

    var projOptions = MaskedProjection.CreateMaskedProjection(taskOptionsFields,
                                                              FieldsToTaskOptions,
                                                              FieldsToTaskOptionsHolder);

    var proj = Expression.Lambda<Func<TaskData, object?>>(projOptions.Body,
                                                          projOptions.Parameters);

    selector_ = MaskedProjection.CreateMaskedProjection(taskDataFields,
                                                        fields => fields == TaskDataFields.Options
                                                                    ? proj
                                                                    : FieldsToTaskData(fields),
                                                        FieldsToTaskDataHolder);
  }

  /// <summary>
  ///   Conversion function from <see cref="TaskDataFields" /> to <see cref="Expression" /> to select the appropriate member
  ///   from <see cref="TaskData" />
  /// </summary>
  /// <param name="field">Field representing the member</param>
  /// <returns>
  ///   <see cref="Expression" /> to select the member
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">when field is not recognized</exception>
  public static Expression<Func<TaskData, object?>> FieldsToTaskData(TaskDataFields field)
  {
    switch (field)
    {
      case TaskDataFields.SessionId:
        return data => data.SessionId;
      case TaskDataFields.TaskId:
        return data => data.TaskId;
      case TaskDataFields.DataDependencies:
        return data => data.DataDependencies;
      case TaskDataFields.DataDependenciesCount:
        return data => data.DataDependencies.Count;
      case TaskDataFields.PayloadId:
        return data => data.PayloadId;
      case TaskDataFields.ParentTaskIds:
        return data => data.ParentTaskIds;
      case TaskDataFields.ParentTaskIdsCount:
        return data => data.ParentTaskIds.Count;
      case TaskDataFields.ExpectedOutputIds:
        return data => data.ExpectedOutputIds;
      case TaskDataFields.ExpectedOutputIdsCount:
        return data => data.ExpectedOutputIds.Count;
      case TaskDataFields.InitialTaskId:
        return data => data.InitialTaskId;
      case TaskDataFields.RetryOfIds:
        return data => data.RetryOfIds;
      case TaskDataFields.RetryOfIdsCount:
        return data => data.RetryOfIds.Count;
      case TaskDataFields.Status:
        return data => data.Status;
      case TaskDataFields.Options:
        return data => data.Options;
      case TaskDataFields.OwnerPodId:
        return data => data.OwnerPodId;
      case TaskDataFields.OwnerPodName:
        return data => data.OwnerPodName;
      case TaskDataFields.StatusMessage:
        return data => data.StatusMessage;
      case TaskDataFields.CreationDate:
        return data => data.CreationDate;
      case TaskDataFields.SubmittedDate:
        return data => data.SubmittedDate;
      case TaskDataFields.StartDate:
        return data => data.StartDate;
      case TaskDataFields.EndDate:
        return data => data.EndDate;
      case TaskDataFields.ReceptionDate:
        return data => data.ReceptionDate;
      case TaskDataFields.AcquisitionDate:
        return data => data.AcquisitionDate;
      case TaskDataFields.ProcessingToEndDuration:
        return data => data.ProcessingToEndDuration;
      case TaskDataFields.CreationToEndDuration:
        return data => data.CreationToEndDuration;
      case TaskDataFields.PodTtl:
        return data => data.PodTtl;
      case TaskDataFields.Output:
        return data => data.Output;
      case TaskDataFields.ProcessedDate:
        return data => data.ProcessedDate;
      case TaskDataFields.ReceivedToEndDuration:
        return data => data.ReceivedToEndDuration;
      default:
        throw new ArgumentOutOfRangeException(nameof(field),
                                              field,
                                              null);
    }
  }

  /// <summary>
  ///   Conversion function from <see cref="TaskDataFields" /> to <see cref="Expression" /> to select the appropriate member
  ///   from <see cref="TaskDataHolder" />
  /// </summary>
  /// <param name="field">Field representing the member</param>
  /// <returns>
  ///   <see cref="Expression" /> to select the member
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">when field is not recognized</exception>
  public static Expression<Func<TaskDataHolder, object?>> FieldsToTaskDataHolder(TaskDataFields field)
  {
    switch (field)
    {
      case TaskDataFields.SessionId:
        return data => data.SessionId;
      case TaskDataFields.TaskId:
        return data => data.TaskId;
      case TaskDataFields.DataDependencies:
        return data => data.DataDependencies;
      case TaskDataFields.DataDependenciesCount:
        return data => data.DataDependenciesCount;
      case TaskDataFields.PayloadId:
        return data => data.PayloadId;
      case TaskDataFields.ParentTaskIds:
        return data => data.ParentTaskIds;
      case TaskDataFields.ParentTaskIdsCount:
        return data => data.ParentTaskIdsCount;
      case TaskDataFields.ExpectedOutputIds:
        return data => data.ExpectedOutputIds;
      case TaskDataFields.ExpectedOutputIdsCount:
        return data => data.ExpectedOutputIdsCount;
      case TaskDataFields.InitialTaskId:
        return data => data.InitialTaskId;
      case TaskDataFields.RetryOfIds:
        return data => data.RetryOfIds;
      case TaskDataFields.RetryOfIdsCount:
        return data => data.RetryOfIdsCount;
      case TaskDataFields.Status:
        return data => data.Status;
      case TaskDataFields.Options:
        return data => data.Options;
      case TaskDataFields.OwnerPodId:
        return data => data.OwnerPodId;
      case TaskDataFields.OwnerPodName:
        return data => data.OwnerPodName;
      case TaskDataFields.StatusMessage:
        return data => data.StatusMessage;
      case TaskDataFields.CreationDate:
        return data => data.CreationDate;
      case TaskDataFields.SubmittedDate:
        return data => data.SubmittedDate;
      case TaskDataFields.StartDate:
        return data => data.StartDate;
      case TaskDataFields.EndDate:
        return data => data.EndDate;
      case TaskDataFields.ReceptionDate:
        return data => data.ReceptionDate;
      case TaskDataFields.AcquisitionDate:
        return data => data.AcquisitionDate;
      case TaskDataFields.ProcessingToEndDuration:
        return data => data.ProcessingToEndDuration;
      case TaskDataFields.CreationToEndDuration:
        return data => data.CreationToEndDuration;
      case TaskDataFields.PodTtl:
        return data => data.PodTtl;
      case TaskDataFields.Output:
        return data => data.Output;
      case TaskDataFields.ProcessedDate:
        return data => data.ProcessedDate;
      case TaskDataFields.ReceivedToEndDuration:
        return data => data.ReceivedToEndDuration;
      default:
        throw new ArgumentOutOfRangeException(nameof(field),
                                              field,
                                              null);
    }
  }


  /// <summary>
  ///   Conversion function from <see cref="TaskOptionsFields" /> to <see cref="Expression" /> to select the appropriate
  ///   member
  ///   from <see cref="TaskData" />
  /// </summary>
  /// <param name="field">Field representing the member</param>
  /// <returns>
  ///   <see cref="Expression" /> to select the member
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">when field is not recognized</exception>
  public static Expression<Func<TaskData, object?>> FieldsToTaskOptions(TaskOptionsFields field)
  {
    switch (field)
    {
      case TaskOptionsFields.ApplicationName:
        return data => data.Options.ApplicationName;
      case TaskOptionsFields.ApplicationNamespace:
        return data => data.Options.ApplicationNamespace;
      case TaskOptionsFields.ApplicationService:
        return data => data.Options.ApplicationService;
      case TaskOptionsFields.ApplicationVersion:
        return data => data.Options.ApplicationVersion;
      case TaskOptionsFields.EngineType:
        return data => data.Options.EngineType;
      case TaskOptionsFields.PartitionId:
        return data => data.Options.PartitionId;
      case TaskOptionsFields.MaxRetries:
        return data => data.Options.MaxRetries;
      case TaskOptionsFields.Priority:
        return data => data.Options.Priority;
      case TaskOptionsFields.MaxDuration:
        return data => data.Options.MaxDuration;
      case TaskOptionsFields.Options:
        return data => data.Options.Options;
      default:
        throw new ArgumentOutOfRangeException(nameof(field),
                                              field,
                                              null);
    }
  }

  /// <summary>
  ///   Conversion function from <see cref="TaskOptionsFields" /> to <see cref="Expression" /> to select the appropriate
  ///   member
  ///   from <see cref="TaskOptionsHolder" />
  /// </summary>
  /// <param name="field">Field representing the member</param>
  /// <returns>
  ///   <see cref="Expression" /> to select the member
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">when field is not recognized</exception>
  public static Expression<Func<TaskOptionsHolder, object?>> FieldsToTaskOptionsHolder(TaskOptionsFields field)
  {
    switch (field)
    {
      case TaskOptionsFields.ApplicationName:
        return holder => holder.ApplicationName;
      case TaskOptionsFields.ApplicationNamespace:
        return holder => holder.ApplicationNamespace;
      case TaskOptionsFields.ApplicationService:
        return holder => holder.ApplicationService;
      case TaskOptionsFields.ApplicationVersion:
        return holder => holder.ApplicationVersion;
      case TaskOptionsFields.EngineType:
        return holder => holder.EngineType;
      case TaskOptionsFields.PartitionId:
        return holder => holder.PartitionId;
      case TaskOptionsFields.MaxRetries:
        return holder => holder.MaxRetries;
      case TaskOptionsFields.Priority:
        return holder => holder.Priority;
      case TaskOptionsFields.MaxDuration:
        return holder => holder.MaxDuration;
      case TaskOptionsFields.Options:
        return holder => holder.Options;
      default:
        throw new ArgumentOutOfRangeException(nameof(field),
                                              field,
                                              null);
    }
  }

  /// <summary>
  ///   Get the projection built from the mask given at class instantiation
  /// </summary>
  /// <returns>
  ///   The <see cref="Expression" /> representing the projection from <see cref="TaskData" /> to
  ///   <see cref="TaskDataHolder" />
  /// </returns>
  public Expression<Func<TaskData, TaskDataHolder>> GetProjection()
    => selector_;
}
