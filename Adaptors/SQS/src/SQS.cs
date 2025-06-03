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

using System.Collections.Generic;

namespace ArmoniK.Core.Adapters.SQS;

internal class SQS
{
  public const string SettingSection = nameof(SQS);

  /// <summary>
  ///   Partition from which tasks are pulled by the polling agent
  /// </summary>
  public string PartitionId { get; set; } = string.Empty;

  /// <summary>
  ///   AWS endpoint containing the SQS instance
  /// </summary>
  // ReSharper disable once InconsistentNaming
  public string ServiceURL { get; set; } = string.Empty;

  /// <summary>
  ///   Prefix to add to the created topics and subscriptions
  /// </summary>
  public string Prefix { get; set; } = string.Empty;

  /// <summary>
  ///   AWS Tags to add to the Queues when they are created
  /// </summary>
  public Dictionary<string, string> Tags { get; set; } = new();

  /// <summary>
  ///   Acknowledgment deadline in seconds: If a message wasn't acknowledged within this deadline, it will be
  ///   redelivered .
  /// </summary>
  public int AckDeadlinePeriod { get; set; } = 120;

  /// <summary>
  ///   Time  in seconds between two modifications of acknowledgment deadline
  /// </summary>
  public int AckExtendDeadlineStep { get; set; } = 60;

  /// <summary>
  ///   SQS long polling wait time in seconds (1-20).
  ///   Set to 0 in order to disable long polling.
  /// </summary>
  public int WaitTimeSeconds { get; set; } = 20;

  /// <summary>
  ///   Parallelism used in the control plane when possible. Defaults to the number of threads.
  /// </summary>
  public int DegreeOfParallelism { get; set; } = 0;

  /// <summary>
  ///   Number of priority levels supported. Each priority level will create its own SQS topic.
  /// </summary>
  public int MaxPriority { get; set; } = 0;

  /// <summary>
  ///   Attributes of the created SQS
  /// </summary>
  /// <remarks>
  ///   Attributes reference can be found in
  ///   <a
  ///     href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#SQS-CreateQueue-request-Attributes">
  ///     AWS documentation
  ///   </a>
  /// </remarks>
  public Dictionary<string, string> Attributes { get; set; } = new();
}
