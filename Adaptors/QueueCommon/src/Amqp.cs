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

using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.QueueCommon;

/// <summary>
///   Represents the configuration settings for the AMQP (Advanced Message Queuing Protocol) connection.
/// </summary>
[PublicAPI]
public class Amqp
{
  /// <summary>
  ///   The name of the configuration section for AMQP settings.
  /// </summary>
  public const string SettingSection = nameof(Amqp);

  /// <summary>
  ///   Hostname of the AMQP server.
  /// </summary>
  public string Host { get; set; } = "";

  /// <summary>
  ///   Path to the credentials file for authentication.
  /// </summary>
  public string CredentialsPath { get; set; } = "";

  /// <summary>
  ///   Username for connecting to the AMQP server.
  /// </summary>
  public string User { get; set; } = "";

  /// <summary>
  ///   Password for connecting to the AMQP server.
  /// </summary>
  public string Password { get; set; } = "";

  /// <summary>
  ///   Scheme (protocol) used for the connection (e.g., "amqp", "amqps").
  /// </summary>
  public string Scheme { get; set; } = "";

  /// <summary>
  ///   Path to the Certificate Authority (CA) file for TLS connections.
  /// </summary>
  public string CaPath { get; set; } = "";

  /// <summary>
  ///   Partition ID used for pulling messages.
  /// </summary>
  public string PartitionId { get; set; } = "";

  /// <summary>
  ///   Port number for the AMQP server connection.
  /// </summary>
  public int Port { get; set; }

  /// <summary>
  ///   Maximum priority level for messages in the queue.
  /// </summary>
  public int MaxPriority { get; set; }

  /// <summary>
  ///   Whether to allow host name mismatches in TLS certificates.
  /// </summary>
  public bool AllowHostMismatch { get; set; }

  /// <summary>
  ///   Maximum number of retry attempts for failed operations.
  /// </summary>
  public int MaxRetries { get; set; }

  /// <summary>
  ///   Link credit for flow control in the AMQP connection. The minimum valued supported is 1
  ///   For more details see:
  ///   <a
  ///     href="https: //www.rabbitmq.com/blog/2024/09/02/amqp-flow-control">
  ///   </a>
  /// </summary>
  public int LinkCredit { get; set; }

  /// <summary>
  ///   Limit on the level of parallelism for operations.
  ///   If parallelismLimit is 0, the number of threads is used as the limit.
  ///   If parallelismLimit is negative, no limit is enforced.
  /// </summary>
  public int ParallelismLimit { get; set; }

  /// <summary>
  ///   Whether to allow insecure TLS connections.
  /// </summary>
  public bool AllowInsecureTls { get; set; }
}
