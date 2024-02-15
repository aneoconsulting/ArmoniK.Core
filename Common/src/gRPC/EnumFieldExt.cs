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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;

using TaskOptionEnumField = ArmoniK.Api.gRPC.V1.Tasks.TaskOptionEnumField;
using TaskOptionGenericField = ArmoniK.Api.gRPC.V1.Tasks.TaskOptionGenericField;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Class to extend gRPC messages to convert them in <see cref="Expression" /> in order to convert them in requests to
///   the database
/// </summary>
public static class EnumFieldExt
{
  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="field">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<TaskData, object?>> ToField(this TaskOptionGenericField field)
    => data => data.Options.Options[field.Field];

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<TaskData, object?>> ToField(this TaskSummaryEnumField enumField)
    => enumField switch
       {
         TaskSummaryEnumField.TaskId                  => data => data.TaskId,
         TaskSummaryEnumField.SessionId               => data => data.SessionId,
         TaskSummaryEnumField.OwnerPodId              => data => data.OwnerPodId,
         TaskSummaryEnumField.InitialTaskId           => data => data.InitialTaskId,
         TaskSummaryEnumField.Status                  => data => data.Status,
         TaskSummaryEnumField.CreatedAt               => data => data.CreationDate,
         TaskSummaryEnumField.SubmittedAt             => data => data.SubmittedDate,
         TaskSummaryEnumField.StartedAt               => data => data.StartDate,
         TaskSummaryEnumField.EndedAt                 => data => data.EndDate,
         TaskSummaryEnumField.CreationToEndDuration   => data => data.CreationToEndDuration,
         TaskSummaryEnumField.ProcessingToEndDuration => data => data.ProcessingToEndDuration,
         TaskSummaryEnumField.PodTtl                  => data => data.PodTtl,
         TaskSummaryEnumField.PodHostname             => data => data.OwnerPodName,
         TaskSummaryEnumField.ReceivedAt              => data => data.ReceptionDate,
         TaskSummaryEnumField.AcquiredAt              => data => data.AcquisitionDate,
         TaskSummaryEnumField.Error                   => data => data.Output.Error,
         TaskSummaryEnumField.ReceivedToEndDuration   => data => data.ReceivedToEndDuration,
         TaskSummaryEnumField.ProcessedAt             => data => data.ProcessedDate,
         TaskSummaryEnumField.FetchedAt               => data => data.FetchedDate,
         TaskSummaryEnumField.Unspecified             => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                            => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };


  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<TaskData, object?>> ToField(this TaskOptionEnumField enumField)
    => enumField switch
       {
         TaskOptionEnumField.MaxDuration          => data => data.Options.MaxDuration,
         TaskOptionEnumField.MaxRetries           => data => data.Options.MaxRetries,
         TaskOptionEnumField.Priority             => data => data.Options.Priority,
         TaskOptionEnumField.PartitionId          => data => data.Options.PartitionId,
         TaskOptionEnumField.ApplicationName      => data => data.Options.ApplicationName,
         TaskOptionEnumField.ApplicationVersion   => data => data.Options.ApplicationVersion,
         TaskOptionEnumField.ApplicationNamespace => data => data.Options.ApplicationNamespace,
         TaskOptionEnumField.ApplicationService   => data => data.Options.ApplicationService,
         TaskOptionEnumField.EngineType           => data => data.Options.EngineType,
         TaskOptionEnumField.Unspecified          => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                        => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<SessionData, object?>> ToField(this SessionRawEnumField enumField)
    => enumField switch
       {
         SessionRawEnumField.SessionId    => session => session.SessionId,
         SessionRawEnumField.Status       => session => session.Status,
         SessionRawEnumField.PartitionIds => session => session.PartitionIds,
         SessionRawEnumField.Options      => session => session.Options,
         SessionRawEnumField.CreatedAt    => session => session.CreationDate,
         SessionRawEnumField.CancelledAt  => session => session.CancellationDate,
         SessionRawEnumField.Duration     => session => session.Duration,
         SessionRawEnumField.ClosedAt     => session => session.ClosureDate,
         SessionRawEnumField.PurgedAt     => session => session.PurgeDate,
         SessionRawEnumField.DeletedAt    => session => session.DeletionDate,
         SessionRawEnumField.Unspecified  => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<SessionData, object?>> ToField(this Api.gRPC.V1.Sessions.TaskOptionEnumField enumField)
    => enumField switch
       {
         Api.gRPC.V1.Sessions.TaskOptionEnumField.MaxDuration          => data => data.Options.MaxDuration,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.MaxRetries           => data => data.Options.MaxRetries,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.Priority             => data => data.Options.Priority,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.PartitionId          => data => data.Options.PartitionId,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.ApplicationName      => data => data.Options.ApplicationName,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.ApplicationVersion   => data => data.Options.ApplicationVersion,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.ApplicationNamespace => data => data.Options.ApplicationNamespace,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.ApplicationService   => data => data.Options.ApplicationService,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.EngineType           => data => data.Options.EngineType,
         Api.gRPC.V1.Sessions.TaskOptionEnumField.Unspecified          => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                                             => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="field">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<SessionData, object?>> ToField(this Api.gRPC.V1.Sessions.TaskOptionGenericField field)
    => data => data.Options.Options[field.Field];

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<Result, object?>> ToField(this ResultRawEnumField enumField)
    => enumField switch
       {
         ResultRawEnumField.SessionId   => result => result.SessionId,
         ResultRawEnumField.Name        => result => result.Name,
         ResultRawEnumField.OwnerTaskId => result => result.OwnerTaskId,
         ResultRawEnumField.Status      => result => result.Status,
         ResultRawEnumField.CreatedAt   => result => result.CreationDate,
         ResultRawEnumField.ResultId    => result => result.ResultId,
         ResultRawEnumField.Size        => result => result.Size,
         ResultRawEnumField.CompletedAt => throw new ArgumentOutOfRangeException(nameof(enumField)),
         ResultRawEnumField.Unspecified => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                              => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<PartitionData, object?>> ToField(this PartitionRawEnumField enumField)
    => enumField switch
       {
         PartitionRawEnumField.Id                   => partitionData => partitionData.PartitionId,
         PartitionRawEnumField.ParentPartitionIds   => partitionData => partitionData.ParentPartitionIds,
         PartitionRawEnumField.PodReserved          => partitionData => partitionData.PodReserved,
         PartitionRawEnumField.PodMax               => partitionData => partitionData.PodMax,
         PartitionRawEnumField.PreemptionPercentage => partitionData => partitionData.PreemptionPercentage,
         PartitionRawEnumField.Priority             => partitionData => partitionData.Priority,
         PartitionRawEnumField.Unspecified          => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                          => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="Expression" /> that represent how to access the field from the database object
  /// </summary>
  /// <param name="enumField">The gRPC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  public static Expression<Func<TaskData, object?>> ToField(this ApplicationRawEnumField enumField)
    => enumField switch
       {
         ApplicationRawEnumField.Name        => taskData => taskData.Options.ApplicationName,
         ApplicationRawEnumField.Version     => taskData => taskData.Options.ApplicationVersion,
         ApplicationRawEnumField.Namespace   => taskData => taskData.Options.ApplicationNamespace,
         ApplicationRawEnumField.Service     => taskData => taskData.Options.ApplicationService,
         ApplicationRawEnumField.Unspecified => throw new ArgumentOutOfRangeException(nameof(enumField)),
         _                                   => throw new ArgumentOutOfRangeException(nameof(enumField)),
       };
}
