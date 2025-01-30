// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.Pollster;

public enum AcquisitionStatus
{
  /// <summary>
  ///   Task successfully acquired
  /// </summary>
  Acquired,

  /// <summary>
  ///   Cancellation token triggered after task read
  /// </summary>
  CancelledAfterFirstRead,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Cancelling" />
  /// </summary>
  TaskIsCancelling,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Processed" />
  /// </summary>
  TaskIsProcessed,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Creating" />
  /// </summary>
  TaskIsCreating,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Error" />
  /// </summary>
  TaskIsError,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Cancelled" />
  /// </summary>
  TaskIsCancelled,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Processing" /> but <see cref="TaskData.OwnerPodId" />
  ///   is empty
  /// </summary>
  TaskIsProcessingPodIdEmpty,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Processing" /> but task is processing elsewhere
  /// </summary>
  TaskIsProcessingElsewhere,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Processing" /> but task is processing here
  /// </summary>
  TaskIsProcessingHere,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Retried" />
  /// </summary>
  TaskIsRetried,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Retried" />. Moreover, the retried task is
  ///   <see cref="TaskStatus.Creating" />
  ///   Retried task finalization is required.
  /// </summary>
  TaskIsRetriedAndRetryIsCreating,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Retried" />. Moreover, the retried task was not found
  ///   in the database
  ///   Retried task creation and submission is required.
  /// </summary>
  TaskIsRetriedAndRetryIsNotFound,

  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Retried" />. Moreover, the retried task is
  ///   <see cref="TaskStatus.Submitted" />
  ///   Reinsertion in the queue may be required.
  /// </summary>
  TaskIsRetriedAndRetryIsSubmitted,


  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Retried" />. Moreover, the retried task is
  ///   <see cref="TaskStatus.Pending" />
  ///   Reinsertion in the queue may be required.
  /// </summary>
  TaskIsRetriedAndRetryIsPending,


  /// <summary>
  ///   Task not acquired because its status is <see cref="TaskStatus.Processing" /> but the other pod does not seem to be
  ///   processing it
  /// </summary>
  TaskIsProcessingButSeemsCrashed,

  /// <summary>
  ///   Task not acquired because tasks from session should not be run anymore
  /// </summary>
  SessionNotExecutable,

  /// <summary>
  ///   Task not acquired because the session has been paused
  /// </summary>
  SessionPaused,

  /// <summary>
  ///   Task not acquired because cancellation token triggered after session state processing
  /// </summary>
  CancelledAfterSessionAccess,

  /// <summary>
  ///   Task not acquired because cancellation token triggered after acquisition
  /// </summary>
  CancelledAfterAcquisition,

  /// <summary>
  ///   Task not acquired because <see cref="TaskData.OwnerPodId" /> is empty
  /// </summary>
  PodIdEmptyAfterAcquisition,

  /// <summary>
  ///   Task acquisition failed and timeout before acquisition is not exceeded
  /// </summary>
  AcquisitionFailedTimeoutNotExceeded,

  /// <summary>
  ///   Task acquisition failed and task is being cancelled
  /// </summary>
  AcquisitionFailedTaskCancelling,

  /// <summary>
  ///   Task acquisition failed and message seems to be duplicated
  /// </summary>
  AcquisitionFailedMessageDuplicated,

  /// <summary>
  ///   Task acquisition failed and task is being processed here
  /// </summary>
  AcquisitionFailedProcessingHere,

  /// <summary>
  ///   Task not acquired because cancellation token triggered precondition are ok
  /// </summary>
  CancelledAfterPreconditions,

  /// <summary>
  ///   Task not acquired because task metadata are not found in database
  /// </summary>
  TaskNotFound,

  /// <summary>
  ///   Task is paused but still in the queue
  /// </summary>
  TaskIsPaused,

  /// <summary>
  ///   Task is missing dependencies but happens to be in the queue
  /// </summary>
  TaskIsPending,

  /// <summary>
  ///   Task has status 'submitted' but it had the status 'dispatched' during the start of acquirement
  /// </summary>
  TaskSubmittedButPreviouslyDispatched,

  /// <summary>
  ///   Task has status 'submitted' but it had the status 'processing' during the start of acquirement
  /// </summary>
  TaskSubmittedButPreviouslyProcessing,
}
