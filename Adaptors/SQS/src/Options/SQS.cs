// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Adapters.SQS;

[ExtractDocumentation("Options for SQS")]
internal class SQS
{
  /// <summary>
  ///   The name of the configuration section for SQS settings.
  /// </summary>
  public const string SettingSection = nameof(SQS);

  /// <summary>
  ///   URL of the AWS endpoint hosting the SQS service.
  ///   Leave empty to use the default AWS endpoint resolution (recommended for production).
  ///   Override for local testing (e.g. <c>http://localhost:4566</c> for LocalStack).
  /// </summary>
  // ReSharper disable once InconsistentNaming
  public string ServiceURL { get; set; } = string.Empty;

  /// <summary>
  ///   String prepended to every queue name created by this adapter.
  ///   Useful to disambiguate queues when multiple ArmoniK deployments share the same AWS account.
  /// </summary>
  public string Prefix { get; set; } = string.Empty;

  /// <summary>
  ///   AWS resource tags applied to each queue at creation time.
  ///   Keys and values must comply with
  ///   <a href="https://docs.aws.amazon.com/tag-editor/latest/userguide/tagging.html">AWS tagging rules</a>.
  /// </summary>
  public Dictionary<string, string> Tags { get; set; } = new();

  /// <summary>
  ///   Visibility timeout in seconds for received messages.
  ///   A message that has not been deleted within this window becomes visible again and will be redelivered.
  ///   Defaults to <c>120</c> seconds.
  /// </summary>
  public int AckDeadlinePeriod { get; set; } = 120;

  /// <summary>
  ///   Interval in seconds at which the visibility timeout is renewed for messages that are still being processed.
  ///   Must be strictly less than <see cref="AckDeadlinePeriod" /> to avoid premature redelivery.
  ///   Allow sufficient buffer time to account for clock skew and processing delays.
  ///   Defaults to <c>60</c> seconds.
  /// </summary>
  public int AckExtendDeadlineStep { get; set; } = 60;

  /// <summary>
  ///   Duration in seconds the adapter waits for messages during a single SQS <c>ReceiveMessage</c> call.
  ///   Valid range is 0–20. <c>0</c> uses short polling; 1–20 uses long polling.
  ///   Long polling is recommended as it reduces empty responses and lowers cost.
  ///   Defaults to <c>20</c> seconds.
  /// </summary>
  public int WaitTimeSeconds { get; set; } = 20;

  /// <summary>
  ///   Maximum number of concurrent operations when the adapter processes items in parallel.
  ///   Set to <c>0</c> to use the number of logical processors on the machine (
  ///   <see cref="System.Environment.ProcessorCount" />).
  /// </summary>
  public int DegreeOfParallelism { get; set; } = 0;

  /// <summary>
  ///   Number of distinct priority levels. One SQS queue is created per priority level.
  ///   Set to <c>0</c> to disable priority-based routing (single queue).
  /// </summary>
  public int MaxPriority { get; set; } = 0;

  /// <summary>
  ///   Additional SQS queue attributes applied at queue creation time (e.g. <c>FifoQueue</c>, <c>KmsMasterKeyId</c>).
  ///   These are merged with any attributes set by the adapter itself; adapter-managed attributes take precedence.
  /// </summary>
  /// <remarks>
  ///   For the full list of supported attributes, see the
  ///   <a
  ///     href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#SQS-CreateQueue-request-Attributes">
  ///     AWS CreateQueue API reference
  ///   </a>
  ///   .
  /// </remarks>
  public Dictionary<string, string> Attributes { get; set; } = new();

  /// <summary>
  ///   Maximum number of retry attempts for transient AWS SDK errors before the operation is considered failed.
  ///   Defaults to <c>5</c>.
  /// </summary>
  public int MaxRetries { get; set; } = 5;

  /// <summary>
  ///   When <c>true</c>, the SQS <c>MessageGroupId</c> attribute is set to the task's session ID,
  ///   ensuring that all messages belonging to the same session are delivered in order on a FIFO queue.
  ///   Requires the target queues to be FIFO queues (suffix <c>.fifo</c>).
  ///   Defaults to <c>false</c>.
  /// </summary>
  public bool UseSessionMessageGroupId { get; set; } = false;
}
