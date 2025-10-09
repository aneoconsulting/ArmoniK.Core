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

using System;

using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Adapters.PubSub;

[ExtractDocumentation("Options for PubSub")]
internal class PubSub
{
  public const string SettingSection = nameof(PubSub);


  /// <summary>
  ///   GCP project id containing the PubSub instance
  /// </summary>
  public string ProjectId { get; set; } = string.Empty;


  /// <summary>
  ///   Prefix to add to the created topics and subscriptions
  /// </summary>
  public string Prefix { get; set; } = string.Empty;

  /// <summary>
  ///   Minimum duration to retain a message after it it published to the topic
  /// </summary>
  public TimeSpan MessageRetention { get; set; } = TimeSpan.FromDays(1);

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
  ///   Name of the KMS key used to protect messages
  /// </summary>
  public string KmsKeyName { get; set; } = string.Empty;

  /// <summary>
  ///   Option to force the ordering of messages (queue property)
  /// </summary>
  public bool MessageOrdering { get; set; } = false;

  /// <summary>
  ///   Guarantee that messages are not duplicated at Pub/Sub level
  /// </summary>
  public bool ExactlyOnceDelivery { get; set; } = false;

  /// <summary>
  ///   Number of priority levels supported. Each priority level will create its own PubSub topic. (Priority 2 will
  ///   potentially be treated before priority 1, it's not absolute)
  /// </summary>
  public int MaxPriority { get; set; } = 0;

  /// <summary>
  ///   Limit on the level of parallelism for operations.
  ///   If DegreeOfParallelism is 0, the number of threads is used as the limit.
  ///   If DegreeOfParallelism is negative, no limit is enforced.
  /// </summary>
  public int DegreeOfParallelism { get; set; }
}
